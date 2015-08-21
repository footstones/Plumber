<?php

namespace Footstones\Plumber;

use Footstones\Plumber\IWorker;
use Footstones\Plumber\BeanstalkClient;
use Footstones\Plumber\Logger;
use swoole_table;
use swoole_process;

class Plumber
{
    private $server;

    private $config;

    protected $logger;

    public function __construct($config)
    {
        $this->config = $config;
    }

    public function main($op)
    {
        $this->{$op}();
    }

    protected function start()
    {

        $this->logger = new Logger(['log_path' => $this->config['log_path']]);

        $this->server = $server = new \swoole_server($this->config['socket_path'], 0, SWOOLE_PROCESS, SWOOLE_UNIX_STREAM);

        $server->set(array(
            'reactor_num' => 1,
            'worker_num' => 1,
            'daemonize' => $this->config['daemonize'],
            'log_file' => $this->config['output_path'],
        ));

        $this->registerMonitorProcess();

        // $this->createTubeListeners($server);

        $server->on('Start', array($this, 'onStart'));
        $server->on('ManagerStart', array($this, 'onManagerStart'));
        $server->on('WorkerStart', array($this, 'onWorkerStart'));
        $server->on('PipeMessage', array($this, 'onPipeMessage'));
        $server->on('Connect', array($this, 'onConnect'));
        $server->on('Receive', array($this, 'onReceive'));
        $server->on('Close', array($this, 'onClose'));
        $server->start();

    }

    protected function stop()
    {

    }

    protected function restart()
    {

    }

    public function onStart( $serv )
    {
        swoole_set_process_name('plumber: master');
        $this->logger->info('plumber master process start.');
    }

    public function onManagerStart(\swoole_server $serv)
    {
        swoole_set_process_name('plumber: manager');
        $this->logger->info('plumber manager process start.');
    }

    public function onWorkerStart($serv, $workerId)
    {
        if($workerId < $serv->setting['worker_num']) {
            swoole_set_process_name("plumber: worker #{$workerId}");
        } else {
            swoole_set_process_name("plumber: task worker #{$workerId}");
        }
    }

    public function onConnect( $serv, $fd, $fromId )
    {
        $this->logger->info('connected from client #{$fd} and reactor #{$fromId}.');
    }

    public function onReceive( $serv, $fd, $fromId, $data )
    {
        $this->logger->info('received message from client #{$fd} and reactor #{$fromId}: '. $data);
        $serv->send($fd, 'ok');
    }

    public function onClose( $serv, $fd, $fromId )
    {
        $this->logger->info('closed from client #{$fd} and reactor #{$fromId}.');
    }

    private function registerMonitorProcess()
    {
        $monitor = new \swoole_process(function($process) {
            $process->name("plumber: monitor");
            $this->createTubeListeners();
            while(true) {
                $result = swoole_process::wait(false);
                if ($result && isset($result['pid'])) {
                    $this->logger->info("process exited.", $result);
                }
                sleep(1);
            }
        }, false, false);

        $monitor->useQueue();
        $monitor->start();

        $this->monitor = $monitor;
    }

    /**
     * 创建队列的监听器
     */
    private function createTubeListeners()
    {
        foreach ($this->config['tubes'] as $tubeName => $tubeConfig) {
            for($i=0; $i<$tubeConfig['worker_num']; $i++) {
                $process = new \swoole_process($this->createTubeLoop($tubeName));
                $process->start();
                // $server->addProcess($process);
            }
        }
    }

    public function onPipeMessage(swoole_server $server, int $from_worker_id, string $message)
    {
        $this->logger->info("PipeMessage: {$message}");
    }

    /**
     * 创建队列处理Loop
     */
    private function createTubeLoop($tubeName)
    {
        return function($process) use ($tubeName) {
            $process->name("plumber: tube `{$tubeName}` task worker");

            $listener = new TubeListener($tubeName, $process, $this->config, $this->logger);
            $listener->connect();

            $beanstalk = $listener->getQueue();

            sleep(20);

            $listener->loop();

            

        };
    }



}