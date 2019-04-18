<?php

namespace Matkomat\Worker;

use Resque;
use Resque_Event;
use Exception;

class ResqueWorkerAbort extends Exception { }

abstract class ResqueWorker implements Worker {

    const DEFAULT_STATUS_TTL = 3*24*60*60; //three days
    const DEFAULT_DELAYED_STATUS_UPDATE_MICROSECONDS = 500000; //0.5 seconds

    const CONTROL_ABORT = 'abort';
    
    /**
     * resque job set by php-resque
     *
     * @var \Resque_Job
     */
    public $job;
    
    /**
     * custom status properties
     *
     * @var array
     */
    protected $status;
    
    /**
     *
     * time to live of status in seconds
     *
     * @var integer
     */
    protected static $statusTtl = self::DEFAULT_STATUS_TTL;
    protected static $delayedStatusUpdateMicroseconds = self::DEFAULT_DELAYED_STATUS_UPDATE_MICROSECONDS; 
    
    protected $statusLastUpdated = null;
    protected static $_onBeforeEnqueueInited = false;
    
    
    protected function initStatus() {
        $this->status = static::getStatusInfo($this->job->payload['id']);
        if (!is_array($this->status)) {
            $this->status = [];
        }
    }
    
    /**
     *
     * wrapper job which will be run by php-resque
     */
    public function perform() {
        $this->init();
        try {
            
            $this->initStatus();
            
            $this->status[StatusInfo::STATUS] = Status::STATUS_WORKING;
            $this->status[StatusInfo::TIME_STARTED] = microtime(true);
            $this->updateStatus();
            
            $this->work();
            
            $this->status[StatusInfo::STATUS] = Status::STATUS_COMPLETED;
            $this->status[StatusInfo::TIME_ENDED] = microtime(true);
            $this->status[StatusInfo::PROGRESS] = 1;
            $this->updateStatus();
        } catch (ResqueWorkerAbort $e) {
            $this->status[StatusInfo::STATUS] = Status::STATUS_ABORTED;
            $this->status[StatusInfo::TIME_ENDED] = microtime(true);
            $this->updateStatus();
            throw $e; //bubble it to resque handler
        } catch (\Throwable $e) {
            $this->status[StatusInfo::STATUS] = Status::STATUS_FAILED;
            $this->status[StatusInfo::TIME_ENDED] = microtime(true);
            $this->status[StatusInfo::ERROR] = $e;
            $this->updateStatus();
            throw $e; //bubble it to resque handler
        }
        
    }
    
    public static function getStatusInfo($jobId) {
        $redis = Resque::redis();
        $key = static::getStatusKey($jobId);
        $status = $redis->get($key);
        return json_decode($status, true);
    }
    
    public static function enqueue($queueName, $jobClass, $args=[]) {
        if (!static::$_onBeforeEnqueueInited) {
            $func = function($jobClass, $args, $queueName, $jobId) {
                $status = [];
                $status[StatusInfo::STATUS] = Status::STATUS_QUEUED;
                $status[StatusInfo::TIME_QUEUED] = microtime(true);
                $redis = Resque::redis();
                $key = static::getStatusKey($jobId);
                $redis->set($key, json_encode($status));
                $redis->expire($key, static::$statusTtl);
                
                $key = static::getJobsByClassIndexKey($jobClass);
                $redis->rpush($key, $jobId);
                $redis->expire($key, static::$statusTtl);
            };
            Resque_Event::listen('beforeEnqueue', $func);
            static::$_onBeforeEnqueueInited = true;
        }
        
        $jobId = Resque::enqueue($queueName, $jobClass, $args, true);
        
        return $jobId;
    }
    
    public static function abort($jobId) {
        $redis = Resque::redis();
        $key = static::getControlKey($jobId, static::CONTROL_ABORT);
        $redis->set($key, 1);
        $redis->expire($key, static::$statusTtl);
    }
    
    protected function getStatusJson() {
        return json_encode($this->status);
    }
    
    public function delayedUpdateStatus($microseconds = null) {
        if (is_null($microseconds)) {
            $microseconds = static::$delayedStatusUpdateMicroseconds;
        }
        if ((microtime(true) - $this->statusLastUpdated) > $microseconds) {
            $this->updateStatus();
        }
    }
    
    public function updateStatus() {
        $redis = Resque::redis();
        $key = static::getStatusKey($this->job->payload['id']);
        $redis->set($key, $this->getStatusJson());
        $redis->expire($key, static::$statusTtl);
        $this->statusLastUpdated = microtime(true);
    }
    
    protected static function getStatusKey($jobId, $subKey=null) {
        $ret = 'customstatus:' . $jobId;
        if (!is_null($subKey)) {
            $ret .=  ':' . $subKey;
        }
        return $ret;
    }
    
    protected static function getControlKey($jobId, $subKey=null) {
        $ret = 'customcontrol:' . $jobId;
        if (!is_null($subKey)) {
            $ret .=  ':' . $subKey;
        }
        return $ret;
    }
    
    protected static function getJobsByClassIndexKey($jobClass, $subKey=null) {
        $ret = 'customjobsindex:'.$jobClass;
        if (!is_null($subKey)) {
            $ret .=  ':' . $subKey;
        }
        return $ret;
    }
    
    public static function getJobsByClass($jobClass, $limit=9999) {
        $redis = Resque::redis();
        $key = static::getJobsByClassIndexKey($jobClass);
        $ret = $redis->lrange($key, -$limit, -1);
        return $ret;
    }
    
    protected function tick() {
        //see if abort requested
        $abortKey = static::getControlKey($this->job->payload['id'], static::CONTROL_ABORT);
        $redis = Resque::redis();
        $abort = $redis->get($abortKey);
        if ($abort) {
            throw new ResqueWorkerAbort();
        }
        
        $this->updateProgressBasedOnPartStates(); 
        
    }
    
    protected function setProgress($progress, $message=null, $delayed = false) {
        $this->tick();
        $this->status[StatusInfo::PROGRESS] = $progress;
        if (!is_null($message)) {
            $this->status[StatusInfo::MESSAGE] = $message;
        }
        if ($delayed) {
            $this->updateStatus();
        } else {
            $this->delayedUpdateStatus();
        }
    }
    
    protected function setMessage($message, $delayed = false) {
        $this->tick();
        $this->status[StatusInfo::MESSAGE] = $message;
        if ($delayed) {
            $this->updateStatus();
        } else {
            $this->delayedUpdateStatus();
        }
    }

    
    public function init() {
        //feel free to override
    }
    
    
    protected $totalParts = 1;
    protected $currentPart = 1;
    protected $currentPartProgress = 0;
    protected $currentPartTimeExpected = 0;
    protected $currentPartStarted = null;

    public function setTotalParts($parts) {
        $this->totalParts = $parts;
    }
    
    public function nextProgressPart($message=null) {
        $this->tick();
        $this->currentPartProgress = 0;
        if (!is_null($message)) {
            $this->status[StatusInfo::MESSAGE] = $message;
        }
        $this->currentPart++;
        $this->currentPartTimeExpected = 0;
        $this->currentPartStarted = time();
        $this->updateStatus();
    }
    
    public function setPartIteration($iteration, $totalIterations) {
        $this->tick();
        $this->currentPartProgress = $iteration / $totalIterations;    
    }
    
    public function setPartTimeExpected($seconds) {
        $this->tick();
        $this->currentPartTimeExpected = $seconds;
        if (is_null($this->currentPartStarted)) {
            $this->currentPartStarted = time();
        }
    }
    
    protected function updateProgressBasedOnPartStates() {
        $onePartTotal = 1 / $this->totalParts;
        $progress = ($this->currentPart - 1) / $this->totalParts;        
        if ($this->currentPartProgress) {
            $progress += $onePartTotal * $this->currentPartProgress;
            $this->status[StatusInfo::PROGRESS] = $progress;
            $this->delayedUpdateStatus();
        } else if ($this->currentPartTimeExpected) {
            $progress += $onePartTotal * ((time() - $this->currentPartStarted) / $this->currentPartTimeExpected);
            $this->status[StatusInfo::PROGRESS] = $progress;
            $this->delayedUpdateStatus();
        }
    }
        
    
    
}