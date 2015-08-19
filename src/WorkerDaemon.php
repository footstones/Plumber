<?php

namespace Footstones\Plumber;

use Beanstalk\Client as BeanstalkClient;

use swoole_table;

class WorkerDaemon
{
    private $lisiteners = array();

    private $idleWorkers = array();

    private $busyWorkers = array();

    private $workerStore;

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
        $this->workerStore = new swoole_table(1024);
        $this->workerStore->column('ids', swoole_table::TYPE_STRING, 4096);
        $this->workerStore->create();

        $socket = realpath(__DIR__ . '/../var/run/worker-daemon.sock');
        $this->server = $server = new \swoole_server($socket, 0, SWOOLE_PROCESS, SWOOLE_UNIX_STREAM);


        $server->set(array(
            'reactor_num' => 1,
            'worker_num' => 1,
            'task_worker_num' => $this->calTaskWorkerNum(),
            'max_connection' => 100,
            'max_request' => 5000,
            'task_max_request' => 5000,
            'daemonize' => 0,
            // 'log_file' => __DIR__ . '/../var/logs/worker-daemon.log',
        ));

        $this->createTubeListeners($server);

        $server->on('Start', array($this, 'onStart'));
        $server->on('ManagerStart', array($this, 'onManagerStart'));
        $server->on('WorkerStart', array($this, 'onWorkerStart'));
        $server->on('PipeMessage', array($this, 'onPipeMessage'));
        $server->on('Connect', array($this, 'onConnect'));
        $server->on('Receive', array($this, 'onReceive'));
        $server->on('Close', array($this, 'onClose'));
        $server->on('Task', array($this, 'onTask'));
        $server->on('Finish', array($this, 'onFinish'));
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
            $this->idleWorkers[$workerId] = 1;
            swoole_set_process_name("plumber: task worker #{$workerId}");
            $this->becomeIdleTaskWorker($workerId);
        }
    }

    public function onFinish($serv,$taskId, $asyncTask)
    {
        $this->writeln("FINISH_TASK\tTask {$taskId} finish");
    }

    public function onPipeMessage($serv, $fromWorkerId, $message)
    {
        $this->writeln("#{$serv->worker_id} onPipeMessage: from #{$fromWorkerId}, {$message}");

        if ($this->isTaskWorker($serv->worker_id)) {
            $this->becomeBusyTaskWorker($serv->worker_id);

            $message = json_decode($message, true);
            $worker = $this->createQueueWorker($message['queue']);
            $worker->execute($message['data']);

            $this->becomeIdleTaskWorker($serv->worker_id);
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

    public function onTask($serv, $taskId, $fromId, $data)
    {
        $this->writeln("OnTask received message:" . $data);
        $serv->finish('finish task');
    }

    private function becomeBusyTaskWorker($workerId)
    {
        $this->workerStore->lock();

        $idleWorkers = $this->workerStore->get('idle');
        $idleWorkers = $idleWorkers ? explode(',', $idleWorkers['ids']) : array();

        $busyWorkers = $this->workerStore->get('busy');
        $busyWorkers = $busyWorkers ? explode(',', $busyWorkers['ids']) : array();

        if (!in_array($workerId, $busyWorkers)) {
            $idleWorkers[] = $workerId;
        }

        if (($key = array_search($workerId, $idleWorkers)) !== false) {
            unset($busyWorkers[$key]);
        }

        $this->workerStore->set('idle', ['ids' => implode(',', $idleWorkers)] );
        $this->workerStore->set('busy', ['ids' => implode(',', $busyWorkers)] );

        $this->workerStore->unlock();
    }

    private function becomeIdleTaskWorker($workerId)
    {
        $this->workerStore->lock();

        $idleWorkers = $this->workerStore->get('idle');
        $idleWorkers = $idleWorkers ? explode(',', $idleWorkers['ids']) : array();

        $busyWorkers = $this->workerStore->get('busy');
        $busyWorkers = $busyWorkers ? explode(',', $busyWorkers['ids']) : array();

        if (!in_array($workerId, $idleWorkers)) {
            $idleWorkers[] = $workerId;
        }

        if (($key = array_search($workerId, $busyWorkers)) !== false) {
            unset($busyWorkers[$key]);
        }

        $this->workerStore->set('idle', ['ids' => implode(',', $idleWorkers)] );
        $this->workerStore->set('busy', ['ids' => implode(',', $busyWorkers)] );

        $this->workerStore->unlock();
    }

    private function hasIdleTaskWorker()
    {
        $idleWorkers = $this->workerStore->get('idle');
        $idleWorkers = $idleWorkers ? explode(',', $idleWorkers['ids']) : array();
        return !empty($idleWorkers);
    }

    private function getAvailableTaskWorker()
    {
        $idleWorkers = $this->workerStore->get('idle');
        $idleWorkers = $idleWorkers ? explode(',', $idleWorkers['ids']) : array();

        $busyWorkers = $this->workerStore->get('busy');
        $busyWorkers = $busyWorkers ? explode(',', $busyWorkers['ids']) : array();

        $workers = $idleWorkers ? : $busyWorkers;

        return array_shift($workers);
    }

    /**
     * 判断worker是否为Task Worker
     */
    private function isTaskWorker($workerId)
    {
        $idleWorkers = $this->workerStore->get('idle');
        $idleWorkers = $idleWorkers ? explode(',', $idleWorkers['ids']) : array();

        $busyWorkers = $this->workerStore->get('busy');
        $busyWorkers = $busyWorkers ? explode(',', $busyWorkers['ids']) : array();

        return array_key_exists($workerId, $idleWorkers) || array_key_exists($workerId, $busyWorkers);
    }

    protected function sendMessageToTaskWorker($tubeName, $message)
    {
        $worker = $this->getAvailableTaskWorker();
        $this->server->sendMessage($message, $worker);
    }

    private function createQueueWorker($name)
    {
        $class = $this->config['tubes'][$name]['class'];
        $worker = new $class();
        return $worker;
    }

    /**
     * 计算Task Worker的数量
     */
    private function calTaskWorkerNum()
    {
        $total = 0;
        foreach ($this->config['tubes'] as $tubeConfig) {
            $total += $tubeConfig['worker_num'];
        }
        return $total;
    }

    /**
     * 创建队列的监听器
     */
    private function createTubeListeners($server)
    {
        $self = $this;
        foreach ($this->config['tubes'] as $tubeName => $tubeConfig) {
            $process = new \swoole_process(function($process) use($tubeName, $self) {
                $process->name("plumber: listen tube `{$tubeName}`");

                $beanstalk = new BeanstalkClient();
                $beanstalk->connect();
                $beanstalk->watch($tubeName);
                while(true) {
                    echo "{$tubeName} reserving.\n";
                    $job = $beanstalk->reserve();
                    echo "{$tubeName} reserved: " . json_encode($job);
                    $message = array('queue' => $tubeName, 'data' => $job);
                    $self->sendMessageToTaskWorker($tubeName, json_encode($message));
                    $beanstalk->delete($job['id']);
                }

            });

            $server->addProcess($process);
            $this->lisiteners[$tubeName] = $process;
        }
    }

    private function writeln($message)
    {
        echo  '[' . date('Y-m-d H:i:s') . '] ' . $message . "\n";
    }

}