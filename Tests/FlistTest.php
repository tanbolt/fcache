<?php
use Tanbolt\Fcache\Flist;

class FlistTest extends \PHPUnit_Framework_TestCase
{
    protected static $file = __DIR__.'/temp/set.dat';

    protected static function clear()
    {
        if (is_file(static::$file)) {
            unlink(static::$file);
        }
    }

    public function testSetKlist()
    {
        static::clear();
        $flist = new Flist(static::$file);

        $this->assertTrue($flist->setValue(['foo1', 'foo2'])->push('foo'));
        $this->assertEquals(['foo1','foo2'], $flist->range('foo'));

        $this->assertTrue($flist->setValue(['foo0'])->insert('foo'));
        $this->assertEquals(['foo0', 'foo1','foo2'], $flist->range('foo'));

        $this->assertTrue($flist->setValue(['bar'])->append('foo', 'foo1'));
        $this->assertEquals(['foo0', 'foo1', 'bar', 'foo2'], $flist->range('foo'));

        $this->assertTrue($flist->setValue(['biz'])->prepend('foo', 'foo1'));
        $this->assertEquals(['foo0', 'biz', 'foo1', 'bar', 'foo2'], $flist->range('foo'));

        $this->assertTrue($flist->setValue(['pdx'])->appendByIndex('foo'));
        $this->assertEquals(['foo0', 'biz', 'foo1', 'bar', 'foo2', 'pdx'], $flist->range('foo'));

        $this->assertTrue($flist->setValue(['pdx_'])->appendByIndex('foo', 2));
        $this->assertEquals(['foo0', 'biz', 'foo1', 'pdx_', 'bar', 'foo2', 'pdx'], $flist->range('foo'));

        $this->assertTrue($flist->setValue(['rdx'])->prependByIndex('foo'));
        $this->assertEquals(['rdx', 'foo0', 'biz', 'foo1', 'pdx_', 'bar', 'foo2', 'pdx'], $flist->range('foo'));

        $this->assertTrue($flist->setValue(['rdx_'])->prependByIndex('foo', 4));
        $this->assertEquals(['rdx', 'foo0', 'biz', 'foo1', 'rdx_', 'pdx_', 'bar', 'foo2', 'pdx'], $flist->range('foo'));

        $flist->close();
        unlink(static::$file);
    }


    public function testReadKList()
    {
        static::clear();
        $flist = new Flist(static::$file);
        $arr = ['f1', 'f2', 'f3', 'f4', 'f5', 'f6'];
        $this->assertTrue($flist->setValue($arr)->push('foo'));

        $this->assertTrue($flist->exist('foo'));
        $this->assertFalse($flist->exist('bar'));

        $this->assertEquals(6, $flist->len('foo'));
        $this->assertEquals(0, $flist->len('bar'));

        $this->assertEquals($arr, $flist->range('foo'));
        $this->assertEquals(array_slice($arr, 1, null, true), $flist->range('foo', 1));
        $this->assertEquals(array_slice($arr, 2, null, true), $flist->range('foo', 2));
        $this->assertEquals(array_slice($arr, 2, 3, true), $flist->range('foo', 2, 3));

        $this->assertEquals(array_slice($arr, -1, null, true), $flist->range('foo', -1));
        $this->assertEquals(array_slice($arr, -3, null, true), $flist->range('foo', -3));
        $this->assertEquals(array_slice($arr, -4, 3, true), $flist->range('foo', -4, 3));

        $this->assertFalse($flist->range('bar'));


        $this->assertEquals(0, $flist->search('foo', 'f1'));
        $this->assertEquals(5, $flist->search('foo', 'f6'));
        $this->assertFalse($flist->search('foo', 'foo'));
        $this->assertFalse($flist->search('bar', 'f1'));

        $flist->close();
        unlink(static::$file);
    }

    public function testAlterKlist()
    {
        static::clear();
        $flist = new Flist(static::$file);
        $arr = ['f1', 'f2', 'f3', 'f4', 'f5', 'f6'];
        $this->assertTrue($flist->setValue($arr)->push('foo'));

        $data = [
            [0, 'f'],
            [3, 'f'],
            [5, 'f'],
            [0, 'fff'],
            [3, 'fff'],
            [5, 'fff']
        ];
        foreach ($data as $item) {
            list($key, $value) = $item;
            $arr[$key] = $value;
            $this->assertTrue($flist->alter('foo', $key, $value));
            $this->assertEquals($arr, $flist->range('foo'));
        }
        $this->assertFalse($flist->alter('bar', 0, 'bar'));

        $flist->close();
        unlink(static::$file);
    }

    public function testPopAndShift()
    {
        static::clear();
        $flist = new Flist(static::$file);

        $arr = ['f1', 'f2', 'f3', 'f4', 'f5', 'f6'];
        $this->assertTrue($flist->setValue($arr)->push('foo'));

        $this->assertEquals(array_pop($arr), $flist->pop('foo'));
        $this->assertEquals($arr, $flist->range('foo'));

        $this->assertEquals(array_pop($arr), $flist->pop('foo'));
        $this->assertEquals($arr, $flist->range('foo'));

        $this->assertEquals(array_shift($arr), $flist->shift('foo'));
        $this->assertEquals($arr, $flist->range('foo'));

        $this->assertEquals(array_shift($arr), $flist->shift('foo'));
        $this->assertEquals($arr, $flist->range('foo'));

        $this->assertFalse($flist->pop('bar'));
        $this->assertFalse($flist->shift('bar'));

        $flist->setValue(['bar'])->push('bar');
        $this->assertEquals('bar', $flist->pop('bar'));
        $this->assertEquals([], $flist->range('bar'));

        $flist->setValue(['biz'])->push('biz');
        $this->assertEquals('biz', $flist->shift('biz'));
        $this->assertEquals([], $flist->range('biz'));

        $flist->close();
        unlink(static::$file);
    }

    public function testRemove()
    {
        static::clear();
        $flist = new Flist(static::$file);
        $arr = ['f1', 'f2', 'f3', 'f4', 'f5', 'f6'];
        foreach ([
            ['foo', 2, null],
            ['bar', -2, null],
            ['biz', 2, 3],
            ['que', -5, 3],
        ] as $item) {
            list($key, $offset, $length) = $item;
            $this->assertTrue($flist->setValue($arr)->push($key));

            $arrTmp = $arr;
            array_splice($arrTmp, $offset, $length === null ? count($arrTmp) : $length);
            $this->assertTrue($flist->remove($key, $offset, $length));
            $this->assertEquals($arrTmp, $flist->range($key));
        }
        $this->assertFalse($flist->remove('test', 0));

        $flist->setValue(['bar'])->push('bar');
        $this->assertTrue($flist->remove('bar', 0));
        $this->assertEquals([], $flist->range('bar'));

        $flist->close();
        unlink(static::$file);
    }

    public function testKeep()
    {
        static::clear();
        $flist = new Flist(static::$file);
        $arr = ['f1', 'f2', 'f3', 'f4', 'f5', 'f6'];

        foreach ([
             ['foo', 2, null],
             ['bar', -2, null],
             ['biz', 2, 3],
             ['que', -5, 3],
        ] as $item) {
            list($key, $offset, $length) = $item;
            $this->assertTrue($flist->setValue($arr)->push($key));

            $arrTmp = array_slice($arr, $offset, $length);
            $this->assertTrue($flist->keep($key, $offset, $length));
            $this->assertEquals($arrTmp, $flist->range($key));
        }
        $this->assertFalse($flist->keep('test', 0));

        $flist->close();
        unlink(static::$file);
    }

    public function testRemoveIndex()
    {
        static::clear();
        $flist = new Flist(static::$file);

        $flist->setValue(['bar'])->push('bar');
        $this->assertTrue($flist->removeIndex('bar', 0));
        $this->assertEquals([], $flist->range('bar'));

        $arr = ['f1', 'f2', 'f3', 'f4', 'f5', 'f6'];
        foreach ([
             0,
             2,
             5,
             [0, 5],
             [0, 2, 5],
             [0, 2],
             [3, 5]
         ] as $index => $item) {
            $arrTmp = $arr;
            if (is_array($item)) {
                foreach ($item as $k) {
                    unset($arrTmp[$k]);
                }
            } else {
                unset($arrTmp[$item]);
            }
            $arrTmp = array_values($arrTmp);

            $key = 'foo'.$index;
            $this->assertTrue($flist->setValue($arr)->push($key));
            $this->assertTrue($flist->removeIndex($key, $item));
            $this->assertEquals($arrTmp, $flist->range($key));
        }
        $this->assertFalse($flist->removeIndex('bar', 0));

        $flist->close();
        unlink(static::$file);
    }

    public function testKeepIndex()
    {
        static::clear();
        $flist = new Flist(static::$file);
        $arr = ['f1', 'f2', 'f3', 'f4', 'f5', 'f6'];
        foreach ([
             0,
             2,
             5,
             [0, 5],
             [0, 2, 5],
             [0, 2],
             [3, 5]
         ] as $index => $item) {
            $arrTmp = [];
            if (is_array($item)) {
                foreach ($item as $k) {
                    $arrTmp[] = $arr[$k];
                }
            } else {
                $arrTmp[] = $arr[$item];
            }

            $key = 'foo'.$index;
            $this->assertTrue($flist->setValue($arr)->push($key));
            $this->assertTrue($flist->keepIndex($key, $item));
            $this->assertEquals($arrTmp, $flist->range($key));
        }
        $this->assertFalse($flist->keepIndex('bar', 0));

        $flist->close();
        unlink(static::$file);
    }

    public function testDrop()
    {
        static::clear();
        $flist = new Flist(static::$file);

        $this->assertFalse($flist->range('foo'));
        $this->assertTrue($flist->setValue(['f'])->push('foo'));
        $this->assertEquals(['f'], $flist->range('foo'));
        $this->assertTrue($flist->drop('foo'));
        $this->assertFalse($flist->range('foo'));

        $flist->close();
        unlink(static::$file);
    }

    public function testSetMultiProcess()
    {
        static::clear();
        $command = 'php '.__DIR__.'/process_set.php list';
        exec($command.' 0', $output1);
        exec($command.' 10000', $output2);
        exec($command.' 20000', $output3);
        $fcache = new Flist(static::$file);
        $this->checkKV($fcache, 30000);
    }

    public function testSetWhenOptimizing()
    {
        static::clear();
        $fcache = new Flist(static::$file);
        for ($i = 0; $i < 30000; $i++) {
            if (!$fcache->setValue([$i.'_'])->push($i)) {
                $this->fail("write [$i] failed");
                $fcache->close();
                unlink(static::$file);
                return;
            }
        }
        $fcache->close();

        $command = 'php '.__DIR__.'/process_optimize.php list';
        exec($command, $output0);

        $command = 'php '.__DIR__.'/process_set.php list';
        exec($command.' 0', $output1);
        exec($command.' 10000', $output2);
        exec($command.' 20000', $output3);

        $fcache = new Flist(static::$file);
        $this->checkKV($fcache, 30000, true);
    }

    protected function checkKV(Flist $fcache, $end, $op = false)
    {
        //get
        for ($i = 0; $i < $end; $i++) {
            $val = $fcache->range($i);
            $act = $op ? [$i.'_', $i.'_'] : [$i.'_'];
            if ($val !== $act) {
                $fcache->close();
                unlink(static::$file);
                $this->fail("get [$i] value [".var_export($val, true)."]");
                return;
            }
        }

        // loop get
        $temp = [];
        for ($i = 0; $i < $end; $i++) {
            $temp[$i] = 0;
        }
        foreach ($fcache as $k => $v) {
            $act = $op ? [$k.'_', $k.'_'] : [$k.'_'];
            if ($v !== $act) {
                $fcache->close();
                unlink(static::$file);
                $this->fail("get [$k] value [".var_export($v, true)."]");
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
