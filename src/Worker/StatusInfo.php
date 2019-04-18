<?php 

namespace Matkomat\Worker;

interface StatusInfo {
    const STATUS = 'status';
    const PROGRESS = 'progress';
    
    const TIME_QUEUED = 'time_queued';
    const TIME_STARTED = 'time_started';
    const TIME_ENDED = 'time_ended';
}
