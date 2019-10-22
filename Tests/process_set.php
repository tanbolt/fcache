<?php
require __DIR__.'/phpunit.php';
use Tanbolt\Fcache\Fkey;
use Tanbolt\Fcache\Flist;
use Tanbolt\Fcache\Fcache;

$type = $argv[1];
$start = (int) $argv[2];

$file = __DIR__.'/temp/set.dat';
if ($type === 'list') {
    $fcache = new Flist($file);
    for ($i = $start; $i < $start + 10000; $i++) {
        $fcache->setValue([$i.'_'])->push($i);
    }
} elseif ($type === 'fkey') {
    $fcache = new Fkey($file);
    for ($i = $start; $i < $start + 10000; $i++) {
        $fcache->add($i);
    }
} else {
    $fcache = new Fcache($file);
    for ($i = $start; $i < $start + 10000; $i++) {
        $fcache->set($i, $i.'_');
    }
}
