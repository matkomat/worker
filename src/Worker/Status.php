<?php 

namespace Matkomat\Worker;

interface Status {
    const STATUS_QUEUED = 1;
    const STATUS_WORKING = 2;
    const STATUS_FAILED = 3;
    const STATUS_COMPLETED = 4;
    const STATUS_ABORTED = 5;
}