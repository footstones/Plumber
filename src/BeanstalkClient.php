<?php

namespace Footstones\Plumber;

use Beanstalk\Client;

class BeanstalkClient extends Client
{
    protected $_latestError;

    public function getLatestError()
    {
        return $this->_latestError;
    }

    protected function _error($message)
    {
        parent::_error($message);
        $this->_latestError = $message;
    }
}