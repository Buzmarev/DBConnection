<?php

namespace Tagesjump\MSSQLConnection\Exception;

use Exception;

class ConnectionException extends Exception
{
    /**
     * @return ConnectionException
     */
    public static function connectionFailed($error)
    {
        return new self("Connection failed with error: " . $error);
    }
}
