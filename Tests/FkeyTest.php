<?php
use Tanbolt\Fcache\Fkey;

class FkeyTest extends \PHPUnit_Framework_TestCase
{
    protected static $file = __DIR__.'/temp/set.dat';

    protected static function clear()
    {
        if (is_file(static::$file)) {
            unlink(static::$file);
        }
    }

    public function testBase()
    {
        static::clear();
        $fkey = new Fkey(static::$file);

        $this->assertFalse($fkey->has('foo'));
        $this->assertTrue($fkey->add('foo'));
        $this->assertTrue($fkey->has('foo'));


        $this->assertFalse($fkey->has('bar'));
        $this->assertTrue($fkey->remove('foo'));
        $this->assertTrue($fkey->remove('bar'));
        $this->assertFalse($fkey->has('foo'));

        $fkey->close();
        unlink(static::$file);
    }


    public function testSetMultiProcess()
    {
        static::clear();
        $command = 'php '.__DIR__.'/process_set.php fkey';
        exec($command.' 0', $output1);
        exec($command.' 10000', $output2);
        exec($command.' 20000', $output3);
        $fcache = new Fkey(static::$file);
        $this->checkKV($fcache, 30000);
    }

    public function testSetWhenOptimizing()
    {
        static::clear();
        $fcache = new Fkey(static::$file);
        for ($i = 0; $i < 30000; $i++) {
            if (!$fcache->add($i)) {
                $this->fail("write [$i] failed");
                $fcache->close();
                unlink(static::$file);
                return;
            }
        }
        $fcache->close();

        $command = 'php '.__DIR__.'/process_optimize.php fkey';
        exec($command, $output0);

        $command = 'php '.__DIR__.'/process_set.php fkey';
        exec($command.' 0', $output1);
        exec($command.' 10000', $output2);
        exec($command.' 20000', $output3);

        $fcache = new Fkey(static::$file);
        $this->checkKV($fcache, 30000);
    }

    protected function checkKV(Fkey $fcache, $end)
    {
        //get
        for ($i = 0; $i < $end; $i++) {
            $has = $fcache->has($i);
            if (!$has) {
                $fcache->close();
                unlink(static::$file);
                $this->fail("get [$i] failed");
                return;
            }
        }

        // loop get
        $temp = [];
        for ($i = 0; $i < $end; $i++) {
            $temp[md5($i)] = 1;
        }
        foreach ($fcache as $k => $v) {
            if (!$v) {
                $fcache->close();
                unlink(static::$file);
                $this->fail("get [$k] value [$v]");
                return;
            }
            if (isset($temp[$k])) {
                unset($temp[$k]);
            }
        }
        $fcache->close();
        unlink(static::$file);
        if (count($temp)) {
            $this->fail("loop get failed");
        } else {
            $this->assertTrue(true);
        }

        // not exist
        $this->assertFalse($fcache->has($end + 1));
    }
}
