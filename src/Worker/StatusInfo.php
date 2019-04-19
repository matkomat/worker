<?php 

namespace Matkomat\Worker;

interface StatusInfo {
    const JOB_ID = 'job_id';
    const JOB_CLASS = 'job_class';
    const STATUS = 'status';
    const PROGRESS = 'progress';
    const MESSAGE = 'message';
    const ERROR = 'error';
    const ARGS = 'args';
    
    const TIME_QUEUED = 'time_queued';
    const TIME_STARTED = 'time_started';
    const TIME_ENDED = 'time_ended';
    
}
