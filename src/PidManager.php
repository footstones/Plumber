<?php

namespace Footstones\Plumber;

class PidManager
{
    private $path;

    public function __construct($path)
    {
        $this->path = $path;
    }

    public function get($name)
    {
        if (!file_exists($this->getFullPath($name))) {
            return 0;
        }
        return intval(file_get_contents($this->getFullPath($name)));
    }

    public function save($name, $pid)
    {
        $pid = intval($pid);
        file_put_contents($this->getFullPath($name), $pid);
    }

    public function clear($name)
    {
        if (!file_exists($this->getFullPath($name))) {
            return;
        }

        unlink($this->getFullPath($name));
    }

    protected function getFullPath($name)
    {
        return $this->path . '.' . $name;
    }
}