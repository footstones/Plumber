#!/usr/bin/env php
<?php

use Beanstalk\Client as BeanstalkClient;

require_once __DIR__.'/../vendor/autoload.php';

$message = json_encode(array('name' => 'Hello'));

$beanstalk = new BeanstalkClient();

$beanstalk->connect();
$beanstalk->useTube('Example1');
$result = $beanstalk->put(
    0, // Give the job a priority of 23.
    0,  // Do not wait to put job into the ready queue.
    60, // Give the job 1 minute to run.
    $message // The job's .body
);

$beanstalk->disconnect();
