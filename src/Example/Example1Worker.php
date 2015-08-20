<?php

namespace Footstones\Plumber\Example;

use Footstones\Plumber\IWorker;

class Example1Worker implements IWorker
{

    public function execute($data)
    {
        echo "example 1";

        sleep(1);

        return array('code' => IWorker::RETRY);
    }

}