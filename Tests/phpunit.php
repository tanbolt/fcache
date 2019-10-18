<?php
date_default_timezone_set('UTC');
if (!class_exists('\PHPUnit_Framework_TestCase') && class_exists('\PHPUnit\Framework\TestCase')) {
    class PHPUnit_Framework_TestCase extends PHPUnit\Framework\TestCase
    {

    }
}

if (!class_exists('\PHPUnit_Framework_TestSuite') && class_exists('\PHPUnit\Framework\TestSuite')) {
    class PHPUnit_Framework_TestSuite extends PHPUnit\Framework\TestSuite
    {

    }
}
class PHPUNIT_LOADER
{
    public static $status = 0;

    /**
     * @var Composer\Autoload\ClassLoader|Tanbolt\Loader
     */
    public static $loader = null;

    /**
     * add namespace
     * @param $prefix
     * @param $file
     * @return bool
     */
    public static function addFile($prefix, $file)
    {
        if (static::$status > 1) {
            if ('\\' == $prefix[0]) {
                $prefix = substr($prefix, 1);
            }
            static::$loader->addClassMap([$prefix => $file]);
            return true;
        }
        if (static::$status > 0) {
            static::$loader->setFile($prefix, $file);
            return true;
        }
        return false;
    }

    /**
     * add namespace group
     * @param $prefix
     * @param $dir
     * @return bool
     */
    public static function addDir($prefix, $dir)
    {
        if (static::$status > 1) {
            if ('\\' == $prefix[0]) {
                $prefix = substr($prefix, 1);
            }
            if ('\\' !== $prefix[strlen($prefix) - 1]) {
                $prefix = $prefix.'\\';
            }
            static::$loader->setPsr4($prefix, $dir);
            return true;
        }
        if (static::$status > 0) {
            static::$loader->setDir($prefix, $dir);
            return true;
        }
        return false;
    }

    /**
     * init auto loader
     */
    public static function init()
    {
        if (ini_get('date.timezone') === '') {
            ini_set('date.timezone','UTC');
        }

        // tanbolt loader
        $file = __DIR__.'/../../loader.php';
        if (is_file($file)) {
            $loader = require $file;
            if (!$loader instanceof Tanbolt\Loader) {
                die('find tanbolt loader file, but class \\Tanbolt\\Loader not exist.');
            }
            $loader->test(true);
            static::$status = 1;
            static::$loader = $loader;
        }

        // composer autoload
        $file = __DIR__.'/../../../autoload.php';
        if (!static::$status && is_file($file)) {
            $loader = require $file;
            if (!$loader instanceof Composer\Autoload\ClassLoader) {
                die('find composer autoload file, but class \\Composer\\Autoload\\ClassLoader not exist.');
            }
            static::$status = 2;
            static::$loader = $loader;
        }

        // die
        if (!static::$status) {
            die('can\'t find composer autoload or tanbolt loader.');
        }
    }
}
PHPUNIT_LOADER::init();
