<?php
use Tanbolt\Fcache\Fcache;

class FcacheTest extends \PHPUnit_Framework_TestCase
{
    protected static $file = __DIR__.'/temp/set.dat';

    protected static function clear()
    {
        if (is_file(static::$file)) {
            unlink(static::$file);
        }
    }

    public function testExpire()
    {
        static::clear();
        $fcache = new Fcache(static::$file);
        $fcache->set('foo', 'foo');
        $this->assertEquals(-1, $fcache->ttl('foo'));

        $fcache->set('bar', 'bar', 100);
        $ttl = $fcache->ttl('bar');
        $this->assertTrue($ttl > 98 && $ttl <= 100);

        $fcache->expire('foo', 600);
        $ttl = $fcache->ttl('foo');
        $this->assertTrue($ttl > 598 && $ttl <= 600);

        $fcache->expire('foo', -1);
        $this->assertNull($fcache->get('foo'));

        $fcache->expire('bar', 0);
        $this->assertEquals(-1, $fcache->ttl('bar'));
        $fcache->close();
        unlink(static::$file);
    }

    public function testIncrease()
    {
        static::clear();
        $fcache = new Fcache(static::$file);

        $this->assertNull($fcache->get('foo'));
        $this->assertEquals(1, $fcache->increase('foo'));
        $this->assertEquals(1, $fcache->get('foo'));
        $this->assertEquals(3, $fcache->increase('foo', 2));
        $this->assertEquals(3, $fcache->get('foo'));

        $this->assertNull($fcache->get('bar'));
        $this->assertEquals(3, $fcache->increase('bar', 3));
        $this->assertEquals(3, $fcache->get('bar'));
        $this->assertEquals(5, $fcache->increase('bar', 2));
        $this->assertEquals(5, $fcache->get('bar'));

        $fcache->close();
        unlink(static::$file);
    }

    public function testSet()
    {
        static::clear();
        $fcache = new Fcache(static::$file);
        for ($i = 0; $i < 1000; $i++) {
            if (!$fcache->set($i, $i.'_')) {
                $this->fail("write [$i] failed");
                $fcache->close();
                unlink(static::$file);
                return;
            }
        }
        $this->checkKV($fcache, 1000);
    }

    public function testSetMultiProcess()
    {
        static::clear();
        $command = 'php '.__DIR__.'/process_set.php kv';
        exec($command.' 0', $output1);
        exec($command.' 10000', $output2);
        exec($command.' 20000', $output3);
        $fcache = new Fcache(static::$file);
        $this->checkKV($fcache, 30000);
    }

    public function testSetWhenOptimizing()
    {
        static::clear();
        $fcache = new Fcache(static::$file);
        for ($i = 0; $i < 30000; $i++) {
            if (!$fcache->set($i, $i)) {
                $this->fail("write [$i] failed");
                $fcache->close();
                unlink(static::$file);
                return;
            }
        }
        $fcache->close();

        $command = 'php '.__DIR__.'/process_optimize.php kv';
        exec($command, $output0);

        $command = 'php '.__DIR__.'/process_set.php kv';
        exec($command.' 0', $output1);
        exec($command.' 10000', $output2);
        exec($command.' 20000', $output3);

        $fcache = new Fcache(static::$file);
        $this->checkKV($fcache, 30000);
    }

    protected function checkKV(Fcache $fcache, $end)
    {
        //get
        for ($i = 0; $i < $end; $i++) {
            $val = $fcache->get($i);
            if ($val !== $i.'_') {
                $fcache->close();
                unlink(static::$file);
                $this->fail("get [$i] value [$val]");
                return;
            }
        }

        // loop get
        $temp = [];
        for ($i = 0; $i < $end; $i++) {
            $temp[$i] = 0;
        }
        foreach ($fcache as $k => $v) {
            if ($v !== $k.'_') {
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
    }
}
