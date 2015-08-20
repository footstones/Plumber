<?php

namespace Footstones\Plumber;

use Beanstalk\Client as BeanstalkClient;

use Footstones\Plumber\IWorker;
use swoole_table;

class WorkerDaemon
{
    private $server;

    private $config;

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
        // var_dump($this->config);exit();
        // if (!file_exists($this->config['socket_path'])) {
        //     touch($this->config['socket_path']);
        //     chmod($this->config['socket_path'], 0777);
        //     $this->config['socket_path'] = realpath($this->config['socket_path']);
        //     var_dump($this->config['socket_path']);
        // }

        $this->server = $server = new \swoole_server($this->config['socket_path'], 0, SWOOLE_PROCESS, SWOOLE_UNIX_STREAM);


        $server->set(array(
            'reactor_num' => 1,
            'worker_num' => 1,
            'daemonize' => $this->config['daemonize'],
            // 'log_file' => $this->config['log_path'],
        ));

        $this->createTubeListeners($server);

        $server->on('Start', array($this, 'onStart'));
        $server->on('ManagerStart', array($this, 'onManagerStart'));
        $server->on('WorkerStart', array($this, 'onWorkerStart'));
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
        $this->writeln("started");
        swoole_set_process_name('plumber: master');
    }

    public function onManagerStart(\swoole_server $serv)
    {
        swoole_set_process_name('plumber: manager');
    }

    public function onWorkerStart($serv, $workerId)
    {
        if($workerId < $serv->setting['worker_num']) {
            swoole_set_process_name("plumber: worker #{$workerId}");
        } else {
            swoole_set_process_name("plumber: task worker #{$workerId}");
            $this->becomeIdleTaskWorker($workerId);
        }
    }

    public function onConnect( $serv, $fd, $fromId )
    {
        $this->writeln("CONNECT\tConnect client {$fd} connect, fromId:{$fromId}.");
    }

    public function onReceive( $serv, $fd, $fromId, $data )
    {
        $this->writeln("RECEIVE\tReceive message from client {$fd}: $data");
        $serv->send($fd, 'ok');
    }

    public function onClose( $serv, $fd, $fromId )
    {
        $this->writeln("CLOSE\tClient {$fd} close server, fromId:{$fromId}");
    }

    /**
     * 创建队列的监听器
     */
    private function createTubeListeners($server)
    {
        $self = $this;
        foreach ($this->config['tubes'] as $tubeName => $tubeConfig) {
            for($i=0; $i<$tubeConfig['worker_num']; $i++) {
                $processInstance = new \swoole_process(function($process) use($tubeName, $self) {
                    $process->name("plumber: tube `{$tubeName}` task worker");

                    $beanstalk = new BeanstalkClient();
                    $beanstalk->connect();
                    $beanstalk->watch($tubeName);
                    $beanstalk->useTube($tubeName);
                    $self->writeln("tube({$tubeName}, P{$process->id}): watching");

                    $worker = $this->createQueueWorker($tubeName);

                    while(true) {
                        $self->writeln("tube({$tubeName}, P{$process->id}): reserving.");
                        $job = $beanstalk->reserve();
                        $job['body'] = json_decode($job['body'], true);
                        $self->writeln("tube({$tubeName}, P{$process->id}): job reserved, ". json_encode($job));

                        $result = $worker->execute($job);
                        $code = is_array($result) ? $result['code'] : $result;

                        switch ($code) {
                            case IWorker::SUCCESS:
                                $deleted = $beanstalk->delete($job['id']);
                                $self->writeln("tube({$tubeName}, P{$process->id}): delete job #{$job['id']}");
                                break;
                            case IWorker::RETRY:
                                $message = $job['body'];
                                if (!isset($message['retry'])) {
                                    $message['retry'] = 0;
                                } else {
                                    $message['retry'] = $message['retry'] + 1;
                                }

                                $jobStat = $beanstalk->statsJob($job['id']);

                                $beanstalk->delete($job['id']);
                                $beanstalk->put(0, 0, 60, json_encode($message));
                                $self->writeln("tube({$tubeName}, P{$process->id}): retry job #{$job['id']}");
                                break;
                            case IWorker::RELEASE:
                            $self->writeln("tube({$tubeName}, P{$process->id}): release job #{$job['id']}");
                                $pri = empty($result['pri']) ? 0 : intval($result['pri']);
                                $delay = empty($result['delay']) ? 0 : intval($result['delay']);
                                $beanstalk->release($job['id'], $pri, $delay);
                                break;
                            case IWorker::ERROR:
                            $self->writeln("tube({$tubeName}, P{$process->id}): bury job #{$job['id']}");
                                $pri = empty($result['pri']) ? 0 : intval($result['pri']);
                                $beanstalk->bury($job['id'], $pri);
                                break;
                            default:
                                break;
                        }

                    }

                });

                $server->addProcess($processInstance);
            }
        }
    }

    private function createQueueWorker($name)
    {
        $class = $this->config['tubes'][$name]['class'];
        $worker = new $class();
        return $worker;
    }

    private function writeln($message)
    {
        echo  '[' . date('Y-m-d H:i:s') . '] ' . $message . "\n";
    }

}