<?php
namespace Footstones\Plumber\Example;

use Footstones\Plumber\WorkerInterface;

class Example2Worker implements WorkerInterface
{

    public function execute($data)
    {
        echo "example 2";
    }

}