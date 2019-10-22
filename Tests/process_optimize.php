<?php
require __DIR__.'/phpunit.php';
use Tanbolt\Fcache\Fkey;
use Tanbolt\Fcache\Flist;
use Tanbolt\Fcache\Fcache;
$type = $argv[1];

$file = __DIR__.'/temp/set.dat';
if ($type === 'list') {
    $flist = new Flist($file);
    $flist->optimize(0);
} elseif ($type === 'fkey') {
    $fcache = new Fkey($file);
    $fcache->optimize(0);
} else {
    $fcache = new Fcache($file);
    $fcache->optimize(0);
}

