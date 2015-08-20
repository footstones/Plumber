<?php
namespace Footstones\Plumber\Example;

use Footstones\Plumber\IWorker;

class Example2Worker implements IWorker
{

    public function execute($data)
    {
        echo "I'm example 2 worker.";
        return array('code' => IWorker::SUCCESS);
    }

}