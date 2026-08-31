<?php

/*
** Same names and values as PHP's built-in SQLite3 constants with the prefix
** swapped, so code ports by search-and-replace.
*/

declare(strict_types=1);

if (!defined('DOLTLITE3_ASSOC')) {
    define('DOLTLITE3_ASSOC', 1);
    define('DOLTLITE3_NUM', 2);
    define('DOLTLITE3_BOTH', 3);

    define('DOLTLITE3_INTEGER', 1);
    define('DOLTLITE3_FLOAT', 2);
    define('DOLTLITE3_TEXT', 3);
    define('DOLTLITE3_BLOB', 4);
    define('DOLTLITE3_NULL', 5);

    define('DOLTLITE3_OPEN_READONLY', 1);
    define('DOLTLITE3_OPEN_READWRITE', 2);
    define('DOLTLITE3_OPEN_CREATE', 4);
}
