#!/usr/bin/env php

<?php

class ManageDaemon
{
    private $workers = array();

    public function run($op)
    {
        $this->{$op}();
    }

    protected function getPid()
    {
        $path = __DIR__ . '/var/pid';
        if (!file_exists($path)) {
            return 0;
        }
        return intval(file_get_contents($path));
    }

    protected function delPid()
    {
        $path = __DIR__ . '/var/pid';
        if (!file_exists($path)) {
            return ;
        }
        unlink($path);
    }

    protected function start()
    {
        $pid = $this->getPid();
        if ($pid) {
            echo "MessageWorker Manager is runing.\n";
            return;
        }

        echo "Start MessageWorker Manager.\n";

        $config = [
            'message_server' => [
                'host' => '127.0.0.1',
                'port' => 11130,
            ],
            'tubes' => [
                'SmsSend' => ['worker_num' => 5],
                'LiveRoomNum' => ['worker_num' => 3]
            ],
        ];

        // swoole_process::daemon();

        $pid = posix_getpid();
        file_put_contents(__DIR__ . '/var/pid', $pid);

        foreach ($config['tubes'] as $tubeName => $tubeConfig) {
            $this->workers[$tubeName] = [];
            for ($i = 0; $i< $tubeConfig['worker_num']; $i++) {

                $worker = $this->createWorker($tubeName, $tubeConfig);
                $pid = $worker->start();

                // $worker = new swoole_process([$this, 'workerProcess'], false, false);
                // $worker->useQueue();
                // $pid = $worker->start();
                $this->workers[$tubeName][$pid] = $worker;
                echo "Process {$tubeName} #{$pid} start.\n";
            }
        }

        swoole_process::signal(SIGTERM, function ($signo) {
            echo "daemon exited.\n";
        });

        swoole_process::signal(SIGCHLD, function($signo) {
            $result = swoole_process::wait();
            if (isset($result['pid'])) {
                echo "PID: {$result['pid']}, code:{$result['code']}, signal:{$result['signal']} exited.\n";
            } else {
                echo "Process exit error.\n";
            }
        });


        while(true) {
            $workers = $this->workers['SmsSend'];
            // shuffle($workers);
            $worker = array_pop($workers);

            echo "PID:{$worker->pid}, Push message.\n";
            $worker->push("hello {$worker->pid}");
            sleep(3);
        }

    }

    protected function createWorker($tubeName, $tubeConfig)
    {
        $class = ucfirst($tubeName) . "Worker";
        return $class($tubeConfig);
    }

    protected function stop()
    {
        $pid = $this->getPid();
        if (empty($pid)) {
            echo "STOP FAILED: daemon is not runing.";
            return ;
        }

        if (swoole_process::kill($pid, 0)) {
            swoole_process::kill($pid, SIGTERM);
        } else {
            echo "STOP FAILED: daemon process is not exist.";
        }

        echo "Stoped.\n";

        $this->delPid();
    }

    protected function restart()
    {
        $this->stop();
        sleep(1);
        $this->start();
    }

    protected function status()
    {

    }

    public function workerProcess(swoole_process $worker)
    {
        while(true) {
            $recv = $worker->pop();
            echo "PID:{$worker->pid}, Pop message: $recv\n";
        }
        $worker->exit(0);
    }

}

if (empty($argv[1]) || !in_array($argv[1], array('start', 'stop', 'restart', 'status')) ) {
    echo "please input: php ManageDaemon start|stop|restart|status\n";
    exit();
}

$daemon = new ManageDaemon();
$daemon->run($argv[1]);