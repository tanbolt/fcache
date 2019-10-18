<?php
require __DIR__.'/phpunit.php';
use Tanbolt\Fcache\Flist;
use Tanbolt\Fcache\Fcache;
$isList = $argv[1] === 'list';

$file = __DIR__.'/temp/set.dat';
if ($isList) {
    $flist = new Flist($file);
    $flist->optimize(0);
} else {
    $fcache = new Fcache($file);
    $fcache->optimize(0);
}

