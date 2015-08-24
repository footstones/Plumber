<?php

namespace Footstones\Plumber;

use Footstones\Plumber\IWorker;
use Footstones\Plumber\BeanstalkClient;
use Footstones\Plumber\Logger;
use Footstones\Plumber\ListenerStats;

use swoole_table;
use swoole_process;

class Plumber
{
    private $server;

    private $config;

    protected $logger;

    protected $pidManager;

    protected $stats;

    public function __construct($config)
    {
        $this->config = $config;
        $this->pidManager = new PidManager($this->config['pid_path']);
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

        $server->on('Start', array($this, 'onStart'));
        $server->on('ManagerStart', array($this, 'onManagerStart'));
        $server->on('WorkerStart', array($this, 'onWorkerStart'));
        $server->on('PipeMessage', array($this, 'onPipeMessage'));
        $server->on('Connect', array($this, 'onConnect'));
        $server->on('Receive', array($this, 'onReceive'));
        $server->on('Close', array($this, 'onClose'));
        $server->on('Shutdown', array($this, 'onShutdown'));

        $this->registerMonitorProcess();
        
        $server->start();

    }

    protected function stop()
    {
        $pid = $this->pidManager->get('master');
        var_dump($pid);
        exec("kill -15 {$pid}");
    }

    protected function restart()
    {

    }

    public function onStart( $serv )
    {
        swoole_set_process_name('plumber: master');
        $this->pidManager->save('master', $serv->master_pid);
        $this->logger->info('plumber master process start.');

    }

    public function onManagerStart(\swoole_server $serv)
    {
        swoole_set_process_name('plumber: manager');
        $this->pidManager->save('manager', $serv->manager_pid);
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

    public function onShutdown($serv)
    {
        $this->logger->info('showdown.');

        $statses = $this->stats->getAll();

        foreach ($statses as $pid => $s) {
            if (swoole_process::kill($pid, 0)) {
                swoole_process::kill($pid, SIGTERM);
            }
        }

        swoole_process::kill($this->monitor->pid, SIGTERM);
    }

    private function registerMonitorProcess()
    {

        $stats = $this->createListenerStats();
        $this->stats = $stats;

        
        $monitor = new \swoole_process(function($process) use ($stats) {
            $process->name("plumber: monitor");
            $this->createTubeListeners($stats);
            $queue = new BeanstalkClient();

            while(true) {
                // 回收已结束的进程
                while(1) {
                    $ret = swoole_process::wait(false);
                    if ($ret) {
                        $this->logger->info("process #{$ret['pid']} exiteddd.", $ret);
                        $st = $this->stats->get($ret['pid']);
                        if ($st && !empty($st['job_id'])) {
                            $queue->connect();
                            $queue->useTube($ret['tube']);
                            $jobStat = $queue->statsJob($ret['job_id']);
                            $released = $queue->release($st['job_id'], $jobStat['pri'], $jobStat['delay']);
                            $this->logger->info("release job #{$ret['job_id']}, {$released}.");
                        }
                    } else {
                        break;
                    }
                }

                $statses = $stats->getAll();
                foreach ($statses as $pid => $s) {
                    if ( ($s['last_update'] + $this->config['reserve_timeout'] + $this->config['execute_timeout']) > time()) {
                        continue;
                    }
                    if (!$s['timeout']) {
                        $this->logger->notice("process #{$pid} last upadte at ". date('Y-m-d H:i:s') . ', it is timeout.', $s);
                        $stats->timeout($pid);
                    }
                }

                sleep(1);
            }
        }, false, false);

        $monitor->useQueue();
        $monitor->start();

        $this->monitor = $monitor;
    }

    private function createListenerStats()
    {
        $size = 0;
        foreach ($this->config['tubes'] as $tubeName => $tubeConfig) {
            $size += $tubeConfig['worker_num'];
        }
        return new ListenerStats($size);
    }

    /**
     * 创建队列的监听器
     */
    private function createTubeListeners($stats)
    {
        foreach ($this->config['tubes'] as $tubeName => $tubeConfig) {
            for($i=0; $i<$tubeConfig['worker_num']; $i++) {
                $process = new \swoole_process($this->createTubeLoop($tubeName, $stats));
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
    private function createTubeLoop($tubeName, $stats)
    {
        return function($process) use ($tubeName, $stats) {
            $process->name("plumber: tube `{$tubeName}` task worker");

            $listener = new TubeListener($tubeName, $process, $this->config, $this->logger, $stats);
            $listener->connect();

            $beanstalk = $listener->getQueue();

            // sleep(10);

            $listener->loop();

        };
    }



}