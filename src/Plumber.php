<?php

namespace Footstones\Plumber;

use Footstones\Plumber\BeanstalkClient;
use Footstones\Plumber\IWorker;
use swoole_table;

class Plumber
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

        $this->server = $server = new \swoole_server($this->config['socket_path'], 0, SWOOLE_PROCESS, SWOOLE_UNIX_STREAM);

        $server->set(array(
            'reactor_num' => 1,
            'worker_num' => 1,
            'daemonize' => $this->config['daemonize'],
            'log_file' => $this->config['run_log_path'],
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
        $this->log("started");
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
        $this->log("CONNECT\tConnect client {$fd} connect, fromId:{$fromId}.");
    }

    public function onReceive( $serv, $fd, $fromId, $data )
    {
        $this->log("RECEIVE\tReceive message from client {$fd}: $data");
        $serv->send($fd, 'ok');
    }

    public function onClose( $serv, $fd, $fromId )
    {
        $this->log("CLOSE\tClient {$fd} close server, fromId:{$fromId}");
    }

    /**
     * 创建队列的监听器
     */
    private function createTubeListeners($server)
    {
        foreach ($this->config['tubes'] as $tubeName => $tubeConfig) {
            for($i=0; $i<$tubeConfig['worker_num']; $i++) {
                $process = new \swoole_process($this->createTubeLoop($tubeName));
                $server->addProcess($process);
            }
        }
    }

    /**
     * 创建队列处理Loop
     */
    private function createTubeLoop($tubeName)
    {
        return function($process) use ($tubeName) {

            $process->name("plumber: tube `{$tubeName}` task worker");

            $beanstalk = new BeanstalkClient();
            $beanstalk->connect();
            $beanstalk->watch($tubeName);
            $beanstalk->useTube($tubeName);
            $this->log("tube({$tubeName}, P{$process->id}): watching.");

            $worker = $this->createQueueWorker($tubeName);

            while(true) {
                $this->log("tube({$tubeName}, P{$process->id}): reserving.");
                $job = $beanstalk->reserve();
                $job['body'] = json_decode($job['body'], true);
                $this->log("tube({$tubeName}, P{$process->id}): job #%d reserved.", $process->id);

                try {
                    $result = $worker->execute($job);
                } catch(\Exception $e) {
                    $message = sprintf('tube({$tubeName}, P%d): job #%d failed, `%s` %s', $process->id, $job['id'], $e->getMessage(), json_encode($job));
                    $this->error($message);
                    continue;
                }

                $code = is_array($result) ? $result['code'] : $result;

                switch ($code) {
                    case IWorker::SUCCESS:
                        $this->log("tube({$tubeName}, P{$process->id}): job #{$job['id']} execute succeed.");

                        $deleted = $beanstalk->delete($job['id']);
                        if (!$deleted) {
                            $this->error("tube({$tubeName}, P{$process->id}): job #{$job['id']} delete failed, in successful executed.");
                        }

                        break;
                    case IWorker::RETRY:

                        $message = $job['body'];
                        if (!isset($message['retry'])) {
                            $message['retry'] = 0;
                        } else {
                            $message['retry'] = $message['retry'] + 1;
                        }
                        $this->log("tube({$tubeName}, P{$process->id}): job #{$job['id']} retry {$message['retry']} times.");

                        $jobStat = $beanstalk->statsJob($job['id']);

                        $beanstalk->delete($job['id']);
                        $beanstalk->put(0, 0, 60, json_encode($message));
                        break;
                    case IWorker::RELEASE:
                    $this->log("tube({$tubeName}, P{$process->id}): release job #{$job['id']}");
                        $pri = empty($result['pri']) ? 0 : intval($result['pri']);
                        $delay = empty($result['delay']) ? 0 : intval($result['delay']);
                        $beanstalk->release($job['id'], $pri, $delay);
                        break;
                    case IWorker::ERROR:
                    $this->log("tube({$tubeName}, P{$process->id}): bury job #{$job['id']}");
                        $pri = empty($result['pri']) ? 0 : intval($result['pri']);
                        $beanstalk->bury($job['id'], $pri);
                        break;
                    default:
                        break;
                }

            }

        };
    }

    private function createQueueWorker($name)
    {
        $class = $this->config['tubes'][$name]['class'];
        $worker = new $class();
        return $worker;
    }

    private function log($message)
    {
        if ($this->config['daemonize']) {

        } else {
            echo  '[' . date('Y-m-d H:i:s') . '] INFO ' . $message . "\n";
        }

    }

    private function error($message)
    {
        if ($this->config['daemonize']) {

        } else {
            echo  '[' . date('Y-m-d H:i:s') . '] ERROR ' . $message . "\n";
        }

    }

}