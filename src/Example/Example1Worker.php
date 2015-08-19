<?php

namespace Footstones\Plumber\Example;

use Footstones\Plumber\WorkerInterface;

class Example1Worker implements WorkerInterface
{

    public function execute($data)
    {
        echo "example 1";
    }

}