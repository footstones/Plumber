<?php

namespace Footstones\Plumber;

use Psr\Log\LoggerInterface;
use Pheanstalk\Pheanstalk;

class ForwardWorker implements IWorker
{
	protected $logger;
	protected $config;
	protected $tubeName;

	public function __construct($tubeName, $config) {
		$this->config = $config;
		$this->tubeName = $tubeName;
	}

	public function execute($job)
	{
		try{
			$body = $job['body'];
			$queue = new BeanstalkClient($this->config['destination']);
			$queue->connect();

			$tubeName = isset($this->config['tubeName']) ? $this->config['tubeName'] : $this->tubeName;
			$queue->useTube($tubeName);

			$pri = isset($config['pri']) ? $config['pri']:0;
			$delay = isset($config['delay']) ? $config['delay']:0;
			$ttr = isset($config['ttr']) ? $config['ttr']:0;
			
			$queue->put($pri, $delay, $ttr, json_encode($data['body']));
			$queue->disconnect();

			return IWorker::FINISH;
		} catch (\Exception $e) {
			$logger->error("job #{$job['id']} forwarded error, job is burried.", $job);
			return IWorker::BURRY;
		}
	}

    public function setLogger(LoggerInterface $logger)
    {
    	$this->logger = $logger;
    }
}
