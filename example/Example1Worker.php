<?php

namespace Footstones\Plumber\Example;

use Footstones\Plumber\IWorker;

class Example1Worker implements IWorker
{

    public function execute($data)
    {
        echo "I'm example 1 worker.\n";
        return array('code' => IWorker::SUCCESS);
    }

}