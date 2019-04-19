<?php

namespace Matkomat\Worker;

use Resque;
use Resque_Event;
use Exception;

class ResqueWorkerAbort extends Exception { }

abstract class ResqueWorker implements Worker {

    const DEFAULT_STATUS_TTL = 3*24*60*60; //three days
    const DEFAULT_DELAYED_STATUS_UPDATE_MILISECONDS = 500; //0.5 seconds

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
    protected static $delayedStatusUpdateMiliseconds = self::DEFAULT_DELAYED_STATUS_UPDATE_MILISECONDS; 
    
    protected $statusLastUpdated = null;
    protected static $_onBeforeEnqueueInited = false;
    
    
    protected function initStatus() {
        $jobId = $this->job->payload['id'];
        $this->status = static::getStatusInfo($jobId);
        if (!$this->statusLastUpdated) {
            $this->statusLastUpdated = 0;
        }
        if (!is_array($this->status)) {
            $this->status = [];
            $this->status[StatusInfo::JOB_ID] = $jobId;
            $this->status[StatusInfo::JOB_CLASS] = get_class($this);
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
            $this->status[StatusInfo::TIME_STARTED] = static::microtime();
            $this->updateStatus();
            
            $this->work();
            
            $this->status[StatusInfo::STATUS] = Status::STATUS_COMPLETED;
            $this->status[StatusInfo::TIME_ENDED] = static::microtime();
            $this->status[StatusInfo::PROGRESS] = 1;
            static::clearExpectedSecondsStatusInfo($this->status);
            $this->updateStatus();
        } catch (ResqueWorkerAbort $e) {
            $this->status[StatusInfo::STATUS] = Status::STATUS_ABORTED;
            $this->status[StatusInfo::TIME_ENDED] = static::microtime();
            static::clearExpectedSecondsStatusInfo($this->status);
            $this->updateStatus();
            throw $e; //bubble it to resque handler
        } catch (\Throwable $e) {
            $this->status[StatusInfo::STATUS] = Status::STATUS_FAILED;
            $this->status[StatusInfo::TIME_ENDED] = static::microtime();
            $this->status[StatusInfo::ERROR] = $e;
            static::clearExpectedSecondsStatusInfo($this->status);
            $this->updateStatus();
            throw $e; //bubble it to resque handler
        }
        
    }
    
    public static function microtime() {
        $redis = Resque::redis();
        $time = $redis->time();
        return $time[0].'.'.$time[1];
    }
    
    public static function getStatusInfo($jobId) {
        $redis = Resque::redis();
        
        $key = static::getStatusKey($jobId);
        $status = $redis->get($key);
        $status = json_decode($status, true);
        
        //see if job is marked as running/queued but resque doesn't have it as running/queued
        if (in_array($status[StatusInfo::STATUS] ?? false, [Status::STATUS_WORKING, Status::STATUS_QUEUED])) {
            $resqueJobStatus = null;
            $rjs = new \Resque_Job_Status($jobId);
            if ($rjs) {
                $resqueJobStatus = $rjs->get();
            }
            if (($resqueJobStatus == \Resque_Job_Status::STATUS_FAILED) || empty($resqueJobStatus)) {
                $status[StatusInfo::STATUS] = Status::STATUS_FAILED;
                $redis->set($key, json_encode($status));
                $redis->expire($key, static::$statusTtl);
                //send abort just in case
                static::abort($jobId);
            }
            
        }
        
        //account the time-based progress
        if (isset($status['_partExpected'])) {
            //account for time
            $status[StatusInfo::PROGRESS] += min([(static::microtime() - $status['_partStarted']) / $status['_partExpected'], 1]) * $status['_partWeight'];
            //static::clearExpectedSecondsStatusInfo($status);
        }
        return $status;
    }
    
    public static function enqueue($jobClass, $args=[], $queueName) {
        if (!static::$_onBeforeEnqueueInited) {
            $func = function($jobClass, $args, $queueName, $jobId) {
                $status = [];
                $status[StatusInfo::JOB_ID] = $jobId;
                $status[StatusInfo::STATUS] = Status::STATUS_QUEUED;
                $status[StatusInfo::TIME_QUEUED] = static::microtime();
                $status[StatusInfo::PROGRESS] = 0;
                $status[StatusInfo::JOB_CLASS] = $jobClass;
                $status[StatusInfo::ARGS] = $args;
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
    
    public function delayedUpdateStatus($miliseconds = null) {
        if (is_null($miliseconds)) {
            $miliseconds = static::$delayedStatusUpdateMiliseconds;
        }
//d('delayed', $miliseconds, static::microtime() - $this->statusLastUpdated);
        if (((static::microtime() - $this->statusLastUpdated)*1000) > $miliseconds) {
            $this->updateStatus();
        }
    }
    
    public function updateStatus() {
        $redis = Resque::redis();
        $key = static::getStatusKey($this->job->payload['id']);
        $redis->set($key, $this->getStatusJson());
        $redis->expire($key, static::$statusTtl);
        $this->statusLastUpdated = static::microtime();
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
    
    
    protected $currentPartWeght = 1;
    protected $currentPartStartProgress = 0;
    public function setPartWeight($weight) {
        $this->currentPartWeght = $weight;
    }
    
    public function setExpectedSeconds($seconds, $partWeight=null) {
        if (!is_null($partWeight)) {
            $this->setPartWeight($partWeight);
        }
        $this->status['_partWeight'] = $this->currentPartWeght;
        $this->status['_partStarted'] = static::microtime();
        $this->status['_partExpected'] = $seconds;
        $this->updateStatus();
    }
    
    public function initialProgressPart($message=null, $partWeight = 0.5, $expectedSeconds = null) {
        $this->tick();
        if (!is_null($message)) {
            $this->status[StatusInfo::MESSAGE] = $message;
        }
        
        $this->setPartWeight($partWeight);
        if (!is_null($expectedSeconds)) {
            $this->setExpectedSeconds($expectedSeconds);
        }
        
        $this->currentPartStartProgress = 0;
        $this->updateStatus();
        
    }
    public function nextProgressPart($message=null, $partWeight = 0.5, $expectedSeconds = null) {
        $this->tick();
        if (!is_null($message)) {
            $this->status[StatusInfo::MESSAGE] = $message;
        }

        $this->setPartWeight($partWeight);
        
        
        if (isset($this->status['_partWeight'])) {
            //add previous part percentage to progress
            if (!isset($this->status[StatusInfo::PROGRESS]) || !is_numeric($this->status[StatusInfo::PROGRESS])) {
                $this->status[StatusInfo::PROGRESS] = 0;
            }
            $this->status[StatusInfo::PROGRESS] += $this->status['_partWeight'];
            static::clearExpectedSecondsStatusInfo($this->status);
        } 
        
        if (!is_null($expectedSeconds)) {
            $this->setExpectedSeconds($expectedSeconds);
        }
        
        $this->currentPartStartProgress = $this->status[StatusInfo::PROGRESS];
        $this->updateStatus();
    }
    
    protected static function clearExpectedSecondsStatusInfo(&$status) {
        unset($status['_partWeight']);
        unset($status['_partStarted']);
        unset($status['_partExpected']);
    }
    
    public function setPartIteration($iteration, $totalIterations) {
        $this->tick();
        $partProgress = $iteration / $totalIterations;   
        if ($partProgress > 1) {
            $partProgress = 1;
        }
        $this->status[StatusInfo::PROGRESS] = $this->currentPartStartProgress + ($partProgress * $this->currentPartWeght);
        
        //by calling this method implementor wants to count iterations, so no need for expectedSeconds route
        if (isset($this->status['_partWeight'])) {
            static::clearExpectedSecondsStatusInfo($this->status);
            $this->updateStatus();
        } else {
            $this->delayedUpdateStatus();
        }
    }
    
    
        
    
    
}