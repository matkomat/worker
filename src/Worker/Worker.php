<?php

namespace Matkomat\Worker;

interface Worker {
    public static function enqueue($queueName, $jobClass, $args=[]);
    public static function abort($jobId);
    public static function getStatusInfo($jobId);
    public static function getJobsByClass($jobClass);
    public function work();
}
