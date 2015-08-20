<?php
namespace Footstones\Plumber\Example;

use Footstones\Plumber\IWorker;

class Example2Worker implements IWorker
{

    public function execute($data)
    {
        echo "example 2";
        return array('code' => IWorker::SUCCESS);
    }

}