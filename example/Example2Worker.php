<?php
namespace Footstones\Plumber\Example;

use Footstones\Plumber\IWorker;
use Psr\Log\LoggerInterface;

class Example2Worker implements IWorker
{

    public function execute($data)
    {
        echo "I'm example 2 worker.";
        return array('code' => IWorker::FINISH);
    }

    public function setLogger(LoggerInterface $logger)
    {
        
    }

}