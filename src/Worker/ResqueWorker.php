<?php

namespace Matkomat\Worker;

use Resque;
use Resque_Event;
use Exception;

class ResqueWorkerAbort extends Exception { }

abstract class ResqueWorker implements Worker {
    
    const CONTROL_ABORT = 'abort';
    
    const DEFAULT_STATUS_TTL = 3*24*60*60; //three days
    
    /**
     * resque job
     *
     * @var array
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
            $this->forceUpdateStatus();
            
            $this->work();
            
            $this->status[StatusInfo::STATUS] = Status::STATUS_COMPLETED;
            $this->status[StatusInfo::TIME_ENDED] = microtime(true);
            $this->forceUpdateStatus();
        } catch (ResqueWorkerAbort $e) {
            $this->status[StatusInfo::STATUS] = Status::STATUS_ABORTED;
            $this->status[StatusInfo::TIME_ENDED] = microtime(true);
            $this->forceUpdateStatus();
            throw $e; //bubble it to resque handler
        } catch (\Throwable $e) {
            $this->status[StatusInfo::STATUS] = Status::STATUS_FAILED;
            $this->status[StatusInfo::TIME_ENDED] = microtime(true);
            $this->forceUpdateStatus();
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
    
    protected $statusLastUpdated = null;
    protected $statusSaveMinimumIntervalMicroseconds = 500000; //0.5 second default
    public function updateStatus() {
        if ((microtime(true) - $this->statusLastUpdated) > $this->statusSaveMinimumIntervalMicroseconds) {
            $this->forceUpdateStatus();
        }
    }
    
    public function forceUpdateStatus($jobId) {
        $redis = Resque::redis();
        $key = static::getStatusKey($jobId);
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
        
    }
    
    protected function setProgress($progress) {
        $this->tick();
        $this->status[StatusInfo::PROGRESS] = $progress;
        $this->updateStatus();
    }
    
    
    public function init() {
        //feel free to override
    }
    
    
}