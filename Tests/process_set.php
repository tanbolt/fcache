<?php
require __DIR__.'/phpunit.php';
use Tanbolt\Fcache\Flist;
use Tanbolt\Fcache\Fcache;

$isList = $argv[1] === 'list';
$start = (int) $argv[2];

$file = __DIR__.'/temp/set.dat';
if ($isList) {
    $fcache = new Flist($file);
    for ($i = $start; $i < $start + 10000; $i++) {
        $fcache->setValue([$i.'_'])->push($i);
    }
} else {
    $fcache = new Fcache($file);
    for ($i = $start; $i < $start + 10000; $i++) {
        $fcache->set($i, $i.'_');
    }
}
