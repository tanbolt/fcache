<?php
namespace Tanbolt\Fcache;

/**
 * Trait Common
 * @package Tanbolt\Fcache
 * 文件链表数据库 全局结构
 *
 * -----------------------------------------------------------------------------------------------------
 * 全局头部: test/status/optimized/createTime/count - 11bit
 * test: 占位符 1bit 在 win 系统下, 写操作 flock 时, 至少 fseek 1 bit 才能读取到数据
 * status: 当前状态 (0:normal, 1:clear, 2:creating, 3:waiting optimize)  1bit
 * optimized: 是否正在优化文件 1bit
 * createTime: 数据库文件的创建时间 4bit
 * count: 记录总数 4bit
 *
 * -----------------------------------------------------------------------------------------------------
 * 索引头部: 0 ~ 0x5FFFF 每4个字节为一个单位, 记录每个链表开始KV的位置信息
 * 每个 key 值都会被转换成为 0 ~ MASK 之间的 hash 数字, 不同 key 值可能会使用相同 hash 数字
 * 每个 kv 存储会记录其 prev next kv 存储位置, 形成链表结构
 * 实测 1千万 数据, 链表长度最长约为 40~50 个, 若数据量巨大, 可考虑使用多个缓存文件进程存储
 *
 * -----------------------------------------------------------------------------------------------------
 * 关于循环读取
 *
 * 1.数据延迟
 * 默认情况下, 循环过程中每次读取 1W 个头部索引
 * 在循环过程中 或 其他进程 写入数据, 会造成头部索引区的读取延迟
 * 可能某个索引头部原本没有数据, 但循环过程中新写入了数据, 原读取仍然认为该位置无数据, 而忽略了该索引区
 * 若对数据实时性较为敏感, 可设置 setIteratorSlice(1) 强制每次读取一个头部
 * 但这同时会降低读取性能, 若对实时性不敏感, 则保持默认即可
 *
 * 2.优化过程中读取
 * 若当前正在进行优化, 使用循环读取大概率会抛出异常, 在循环读取前务必通过 isOptimizing() 先行判断
 *
 */
abstract class Handler implements \Iterator, \Countable
{
    const HANDLER = 0;
    const HANDLER_GET = 1;
    const HANDLER_SET = 2;
    const HANDLER_OPTIMIZE = 5;

    // 桶数量, 值越大, 初始文件越大, 链表可能长度越低
    const MASK = 0x8FFFF;

    // 限制链表长度长度, 当大于某个值时, 不予保存
    const LINK = 0;

    /**
     * @var string
     */
    protected $path;

    /**
     * @var bool
     */
    protected $phpPath;

    /**
     * @var bool
     */
    protected $quiet;

    /**
     * @var resource
     */
    protected $handler;

    /**
     * if on optimizing
     * @var bool
     */
    protected $optimizing = null;

    /**
     * optimize temp handler for normal process
     * @var resource
     */
    protected $optimizedHandler = null;

    /**
     * 优化进程 临时文件指针
     * @var resource
     */
    protected $opHandler = null;

    /**
     * 优化进程回调函数
     * @var callable
     */
    protected $opCallback = null;

    /**
     * 优化时, 是否逐个读取优化进程的缓存文件
     * 若在优化过程中, 有其他进程对缓存文件进行重写, 务必设置该项
     * @var bool
     */
    protected $opOneByOne = false;

    /**
     * @var int
     */
    private $iteratorSlice = null;

    /**
     * @var int
     */
    private $startSlice = null;

    /**
     * @var array|null
     */
    private $currentCache = null;

    /**
     * @var int
     */
    private $currentSlice = -1;

    /**
     * @var int
     */
    private $currentIndex = -1;

    /**
     * @var array
     */
    private $currentGroup = [];

    /**
     * @var int
     */
    private $currentPosition = 0;

    /**
     * @var int
     */
    private $currentPrevPosition = 0;

    /**
     * @var int
     */
    private $currentNextPosition = 0;

    /**
     * @var int
     */
    private $currentLink = 0;

    /**
     * Handler constructor.
     * @param null $path 数据库保存文件路径
     * @param bool $quiet
     */
    public function __construct($path = null, $quiet = false)
    {
        if ($path) {
            $this->setPath($path);
        }
        $this->quiet($quiet);
    }

    /**
     * 设置数据库保存文件夹路径
     * @param string $path 数据库保存文件路径
     * @return $this
     */
    public function setPath($path)
    {
        if ($path) {
            $pathInfo = pathinfo((string) $path);
            $dir = realpath($pathInfo['dirname']);
            $name = $pathInfo['basename'];
            $path = $dir ? $dir . DIRECTORY_SEPARATOR . $name : $path;
            if ($this->path && $this->path === $path) {
                return $this;
            }
            $this->phpPath = isset($pathInfo['extension']) && strtolower($pathInfo['extension']) === 'php';
        } else {
            $this->phpPath = false;
        }
        $this->close()->path = $path;
        return $this;
    }

    /**
     * 获取数据库保存文件路径
     * @return string 文件路径
     */
    public function getPath()
    {
        return $this->path;
    }

    /**
     * 不显示 warning 信息
     * @param bool $quiet
     * @return $this
     */
    public function quiet($quiet = true)
    {
        $this->quiet = (bool) $quiet;
        return $this;
    }

    /**
     * @param null $error
     * @return bool
     */
    protected function setError($error = null)
    {
        if (!$this->quiet) {
            trigger_error($error ?: 'Cache file ['.$this->path.'] is error', E_USER_NOTICE);
        }
        return false;
    }

    /**
     * 创建缓存文件
     * @return bool
     */
    public function create()
    {
        return (bool) $this->getHandler(self::HANDLER_SET);
    }

    /**
     * get file resource handler
     * @param int $type
     * @param int $tries
     * @return resource|false
     */
    protected function getHandler($type = self::HANDLER, $tries = 0)
    {
        if (!$this->path) {
            return $this->setError('Cache data file not configure');
        }
        if ($this->handler) {
            return $this->checkHandler($this->handler, $type, $tries);
        }
        clearstatcache();
        $exist = is_file($this->path);
        if (!$exist) {
            if ($type !== self::HANDLER_SET) {
                return false;
            }
            touch($this->path);
            chmod($this->path, 0664);
        }
        $this->handler = fopen($this->path, 'rb+');
        if (!$this->handler) {
            return $this->setError('Open file ['.$this->path.'] failed');
        }
        if ($exist) {
            return $this->checkHandler($this->handler, $type, $tries);
        }
        return $this->createHandler($this->handler);
    }

    /**
     * 创建初始数据库
     * @param $handler
     * @param int $optimized
     * @return resource|false
     */
    protected function createHandler($handler, $optimized = 0)
    {
        ftruncate($handler, 0);
        fseek($handler, 0, SEEK_SET);
        $header = $this->phpPath ? '<?php exit;?>' : '';
        // header: test/status/optimized/createTime/count
        // status (0:normal, 1:clear, 2:creating, 3:waiting optimize)
        $header .= '02'.$optimized.pack('VV', time(), 0);
        fwrite($handler, $header);
        $pack = pack('V', 0);
        $slices = floor(self::MASK / 4000);
        for ($i = 0; $i < $slices; $i++) {
            fwrite($handler, str_repeat($pack, 4000));
        }
        fwrite($handler, str_repeat($pack, self::MASK - 4000 * $slices));
        fseek($handler, $this->getHeaderLen(1), SEEK_SET);
        fwrite($handler, '0');
        rewind($handler);
        $stat = fstat($handler);
        if ($stat['size'] !== static::MASK * 4 + $this->getStartLength()) {
            fclose($handler);
            unlink($this->path);
            return $this->setError('Create file ['.$this->path.'] failed');
        }
        return $handler;
    }

    /**
     * 检测数据库状态, 根据情况返回可用数据库文件指针 或 false
     * @param $handler
     * @param int $type
     * @param int $tries
     * @return resource|false
     */
    protected function checkHandler($handler, $type = self::HANDLER, $tries = 0)
    {
        if ($type === self::HANDLER) {
            return $handler;
        }
        if ($tries > 100) {
            return $this->setError('Cache ['.$this->path.'] is used by other process');
        } elseif ($tries > 0) {
            usleep(20000);
        }
        fseek($handler, $this->getHeaderLen(1), SEEK_SET);
        $status = fread($handler, 1);
        // 可能在创建过程中, 头部未创建完毕
        if (!strlen($status)) {
            return $this->getHandler($type, ++$tries);
        }
        $status = (int) $status;
        switch ($status) {
            case 1:
                // clearing -> 重写文件; 读操作:直接返回false, 写操作:等待
                $this->createHandler($handler);
                if ($type !== self::HANDLER_SET) {
                    return false;
                }
                return $this->getHandler($type, ++$tries);
            case 2:
                // creating -> 读操作:直接返回false, 写操作:等待
                if ($type !== self::HANDLER_SET) {
                    return false;
                }
                return $this->getHandler($type, ++$tries);
            case 3:
                // waiting optimize -> 优化进程:直接返回false, 常规进程:检测优化锁
                if ($type === self::HANDLER_OPTIMIZE) {
                    return false;
                }
                return $this->checkHandlerOptimize($handler, $type);
        }
        // 是否正在优化过程中, 有可能之前打开的 handler 还未关闭, 包含数据的文件就被改名, 新创建文件同名
        // 但读写指针已发送变化, 需要重新打开
        $this->optimizing = (int) fread($handler, 1);
        if ($this->optimizing && !is_resource($handler)) {
            return $this->close()->getHandler($type);
        }
        return $handler;
    }

    /**
     * 检测优化进程锁
     * @param $handler
     * @param int $type
     * @param int $tries
     * @return resource|false
     */
    protected function checkHandlerOptimize($handler, $type = self::HANDLER, $tries = 0)
    {
        clearstatcache();
        if (!is_file($this->lockFile())) {
            // 未创建 lock file 当前状态就不正确, 认为没有进行优化
            if (is_resource($handler)) {
                $seek = $this->getHeaderLen(1);
                fseek($handler, $seek, SEEK_SET);
                if ((int) fread($handler, 1) === 3) {
                    fseek($handler, $seek, SEEK_SET);
                    fwrite($handler, '0');
                }
            }
            return $this->getHandler($type);
        }
        if ($tries > 30) {
            return $this->setError('Cache ['.$this->path.'] is busy for optimize process');
        }
        // 关闭文件指针, 给优化执行进程修改文件名一个时间窗口
        $this->close();
        usleep(100000);
        return $this->checkHandlerOptimize($handler, $type, ++$tries);
    }

    /**
     * 判断当前是否正在优化过程中
     * @return bool
     */
    public function isOptimizing()
    {
        if (!$this->getHandler()) {
            return false;
        }
        return (bool) $this->optimizing;
    }

    /**
     * 普通进程 获取当前正在执行优化的 临时文件指针
     * @return resource|false
     */
    protected function getOptimizeHandler()
    {
        if (!$this->optimizing) {
            $this->closeOptimizeHandler();
            return false;
        }
        if ($this->optimizedHandler) {
            return $this->optimizedHandler;
        }
        $opFile = $this->opFile();
        $this->optimizedHandler = is_file($opFile) ? fopen($opFile, 'rb+') : null;
        if ($this->optimizedHandler) {
            return $this->optimizedHandler;
        }
        if ($this->handler) {
            // reset header optimized
            fseek($this->handler, $this->getHeaderLen(2), SEEK_SET);
            fwrite($this->handler, '0');
        }
        return false;
    }

    /**
     * 关闭正在执行优化的 临时文件指针
     * @param null $optimizing
     * @return $this
     */
    protected function closeOptimizeHandler($optimizing = null)
    {
        if ($this->optimizedHandler) {
            if (is_resource($this->optimizedHandler)) {
                fclose($this->optimizedHandler);
            }
            $this->optimizedHandler = null;
        }
        if ($optimizing !== null) {
            $this->optimizing = (bool) $optimizing;
        }
        return $this;
    }

    /**
     * 设置循环读取过程中 每次读取头部索引的个数 默认为 10000
     * 个数大 读取性能高, 读取完头部索引会根据索引读取键值
     * 在读取已取出索引键值过程中 可能头部索引被其他进程写入修改 会读到脏数据
     * 若无法容忍脏数据  可设置为 1
     * @param $slice
     * @return $this
     */
    public function setIteratorSlice($slice)
    {
        $this->iteratorSlice = (int) $slice;
        return $this;
    }

    /**
     * @return $this
     */
    public function rewind()
    {
        $this->currentSlice = -1;
        return $this->rewindIterator();
    }

    /**
     * @return bool
     */
    public function valid()
    {
        if ($this->currentSlice < 0) {
            $this->rewindIterator();
        }
        return  $this->currentCache !== null;
    }

    /**
     * @return string
     */
    public function key()
    {
        if ($this->currentSlice < 0) {
            $this->rewindIterator();
        }
        return $this->currentCache === null ? null : key($this->currentCache);
    }

    /**
     * @return mixed
     */
    public function current()
    {
        if ($this->currentSlice < 0) {
            $this->rewindIterator();
        }
        return $this->currentCache === null ? null : current($this->currentCache);
    }

    /**
     * @return $this
     */
    public function next()
    {
        if ($this->currentSlice < 0) {
            $this->rewindIterator();
        }
        $this->currentCache = null;
        if ($this->currentNextPosition > 0) {
            $this->currentPrevPosition = $this->currentPosition;
            $this->currentPosition = $this->currentNextPosition;
            return $this->readNextValue();
        }
        return $this->readNextPosition();
    }

    /**
     * @return $this
     */
    protected function rewindIterator()
    {
        $this->currentSlice = 0;
        $this->currentGroup = [];
        $this->currentCache = null;
        $this->currentPosition = 0;
        $this->startSlice = is_int($this->iteratorSlice) && $this->iteratorSlice > 0 ? $this->iteratorSlice : 10000;
        return $this->readNextPosition();
    }

    /**
     * 读取下一个索引头部, 并进而读取下一个值
     * @return $this
     */
    protected function readNextPosition()
    {
        if (!$this->currentGroup) {
            // 一次性读取一个或多个索引头部
            // 若读取多个索引头部: 有可能在循环读取过程中, 索引头部记录的位置信息发生了变化,会造成读取脏数据
            // 可使用 setIteratorSlice(1) 强制每次读取一个索引头部
            $slice = $this->startSlice;
            $maxSlice = (int) ($slice > 1 ? ceil(self::MASK / $slice) : self::MASK);
            if ($this->currentSlice >= $maxSlice) {
                return $this;
            }
            if (!($handler = $this->opHandler ?: $this->getHandler(self::HANDLER_GET))) {
                $this->setError('Get read handler failed');
                return $this;
            }
            while ($this->currentSlice < $maxSlice) {
                if ($this->currentSlice === $maxSlice - 1) {
                    $count = self::MASK - $slice * $this->currentSlice;
                } else {
                    $count = $slice;
                }
                fseek($handler, $this->currentSlice * $slice * 4 + $this->getStartLength(), SEEK_SET);
                $header = unpack('V'.$count, fread($handler, $count * 4));
                if (!is_array($header) || !isset($header[1]) || !isset($header[$count])) {
                    $this->setError('Get header index failed');
                    return $this;
                }
                $this->currentSlice++;
                if ($header = array_filter($header)) {
                    $this->currentGroup = $header;
                    break;
                }
            }
        }
        $this->currentLink = 0;
        $this->currentPrevPosition = 0;
        $this->currentPosition = reset($this->currentGroup);
        $this->currentIndex = key($this->currentGroup);
        unset($this->currentGroup[$this->currentIndex]);
        return $this->readNextValue();
    }

    /**
     * 读取指定 position 位置的数据
     * @return $this
     */
    protected function readNextValue()
    {
        if (!($handler = $this->opHandler ?: $this->getHandler(self::HANDLER_GET))) {
            return $this;
        }
        $position = $this->currentPosition;
        $this->currentPosition = $this->currentNextPosition = 0;
        if (self::LINK && $this->currentLink >= self::LINK) {
            return $this->next();
        }
        while ($position > 0) {
            $item = $this->readValueForIterator($handler, $position, (bool) $this->opHandler);
            // 正确读取到了数据
            if (is_array($item)) {
                $val = $item['value'];
                if ($this->opHandler) {
                    $index = $this->startSlice * ($this->currentSlice - 1) + $this->currentIndex - 1;
                    $val = [$index, $val];
                }
                $this->currentLink++;
                $this->currentPosition = $position;
                $this->currentNextPosition = $item['next'];
                $this->currentCache = [$item['key'] => $val];
                return $this;
            }
            // 在该 position 无法读取数据, 原因可能是新写入了数据(position 发生变动), 或被删除了
            // 重新读取上一个数据的信息, 尝试获取新的 position
            if ($this->currentPrevPosition) {
                $newPosition = $this->readValueForIterator($handler, $this->currentPrevPosition, false, true);
                $this->currentPrevPosition = 0;
                if ($newPosition > 0 && $newPosition !== $position) {
                    $this->currentPosition = $newPosition;
                    return $this->readNextValue();
                }
            }
            $this->currentLink++;
            $position = (int) $item;
            if (self::LINK && $this->currentLink >= self::LINK) {
                return $this->readNextPosition();
            }
        }
        return $this->readNextPosition();
    }

    /**
     * 返回所有值，数据量大将占用较大内存，不推荐使用
     * 推荐使用 foreach 或 Iterator 方式
     * @return array
     */
    public function all()
    {
        $cache = [];
        $this->rewind();
        while ($this->valid()) {
            $cache[$this->key()] = $this->current();
            $this->next();
        }
        return $cache;
    }

    /**
     * 优化数据库，移除冗长数据 (同一个 key 可能会保存多份历史数据, 仅最后一份数据有效, 优化后仅保留最后一份数据)
     * @param int $miniInterval  距离上次优化的秒数，若在限定时间内，不进行优化
     * @param callable|null $callback  优化回调函数 callback(percent) 返回当前优化进度百分比
     * @return bool
     * @throws \Exception
     * @throws \Throwable
     */
    public function optimize($miniInterval = 7200, callable $callback = null)
    {
        clearstatcache();
        $this->opCallback = null;
        $opFile = $this->opFile();
        if (is_file($opFile) || !($handler = $this->getHandler(self::HANDLER_OPTIMIZE))) {
            // 正在优化过程中
            $this->close();
            return true;
        }
        $this->handler = null;
        // 刚刚优化过
        fseek($handler, $this->getHeaderLen(3), SEEK_SET);
        $createTime = unpack('V', fread($handler, 4));
        if (!is_array($createTime) || !isset($createTime[1])) {
            fclose($handler);
            return $this->setError('Get last optimize time failed');
        }
        $createTime = (int) $createTime[1];
        if ($createTime + $miniInterval > time()) {
            fclose($handler);
            return true;
        }
        // touch lock file
        if (!touch($this->lockFile())) {
            fclose($handler);
            return $this->setError('Create lock file failed');
        }
        // 修改当前缓存文件状态: waiting optimize
        if (fseek($handler, $this->getHeaderLen(1), SEEK_SET) < 0 ||
            !$this->writeBufferToHandler($handler, '3')
        ) {
            fclose($handler);
            unlink($this->lockFile());
            return $this->setError('Modify cache file header failed');
        }
        fclose($handler);
        if ($time = (int) ini_get('max_execution_time')) {
            set_time_limit(0);
        }
        $this->opCallback = $callback;
        $optimized = $this->startOptimize();
        if ($time) {
            set_time_limit($time);
        }
        return $optimized;
    }

    /**
     * rename current data file, create new data file, move old data to new data file
     * @param int $tries
     * @return bool
     * @throws \Exception
     * @throws \Throwable
     */
    private function startOptimize($tries = 0)
    {
        // 有可能文件正在使用, 无法改名, 多次尝试
        $opFileName = $this->opFile();
        set_error_handler(function(){});
        $rename = rename($this->path, $opFileName);
        restore_error_handler();
        if (!$rename) {
            // try again
            if ($tries > 200) {
                unlink($this->lockFile());
                return $this->setError('Change cache file name ['.$this->path.'] to ['.$opFileName.'] failed');
            }
            usleep(10000);
            return $this->startOptimize(++$tries);
        }
        // 改名成功后, 立即以原文件名创建缓存文件, 以保证线上应用
        touch($this->path);
        chmod($this->path, 0664);
        $handler = fopen($this->path, 'rb+');
        if (!$handler) {
            // back status
            $handler = fopen($this->opFile(), 'rb+');
            fseek($handler, $this->getHeaderLen(1), SEEK_SET);
            $this->writeBufferToHandler($handler, 0);
            fclose($handler);
            rename($this->opFile(), $this->path);
            return $this->setError('ReCreate cache file failed');
        }
        // change cache header: optimized=1
        $handler = $this->createHandler($handler, 1);
        $opHandler = fopen($this->opFile(), 'rb+');
        // 开始将就缓存文件中的数据 写入 到新创建缓存文件
        try {
            unlink($this->lockFile());
            return $this->execOptimize($handler, $opHandler);
        } catch (\Exception $e) {
            $this->backOptimizeHeader($handler, $opHandler);
            throw $e;
        } catch (\Throwable $e) {
            $this->backOptimizeHeader($handler, $opHandler);
            throw $e;
        }
    }

    /**
     * @param resource $handler
     * @param resource $opHandler
     * @return bool
     */
    private function execOptimize($handler, $opHandler)
    {
        static $percent = 0;
        if ($this->opCallback) {
            call_user_func($this->opCallback, $percent);
        }
        $this->opHandler = $opHandler;
        if ($this->opOneByOne) {
            $this->setIteratorSlice(1);
        }
        $this->rewind();
        while ($this->valid()) {
            // check if already clear
            if ($this->checkIfOptimizeClear($handler, $opHandler)) {
                return true;
            }
            $key = $this->key();
            list($index, $value) = $this->current();
            if (strlen($key)) {
                $this->writeOptimize($handler, $key, $index, $value);
            }
            if ($this->opCallback) {
                $currentPercent = (int) floor($index * 100 / self::MASK);
                if ($currentPercent !== $percent) {
                    $percent = $currentPercent;
                    call_user_func($this->opCallback, $percent);
                }
            }
            $this->next();
        }
        // back cache header: optimized=0
        return $this->completeOptimize($handler, $opHandler);
    }

    /**
     * cleared when optimizing
     * @param $handler
     * @param $opHandler
     * @return bool
     */
    private function checkIfOptimizeClear($handler, $opHandler)
    {
        fseek($opHandler, $this->getHeaderLen(1), SEEK_SET);
        $status = fread($opHandler, 1);
        if (strlen($status) && (int) $status === 1) {
            return $this->completeOptimize($handler, $opHandler);
        }
        return false;
    }

    /**
     * @param $handler
     * @param $opHandler
     * @return bool
     */
    private function completeOptimize($handler, $opHandler)
    {
        $this->backOptimizeHeader($handler, $opHandler);
        if ($this->opCallback) {
            call_user_func($this->opCallback, 100);
        }
        return true;
    }

    /**
     * change cache header: optimized=0
     * @param $handler
     * @param $opHandler
     * @return bool
     */
    private function backOptimizeHeader($handler, $opHandler)
    {
        $this->opHandler = null;
        fseek($handler, $this->getHeaderLen(2), SEEK_SET);
        fwrite($handler, '0');
        fclose($handler);
        fclose($opHandler);
        return $this->removeOptimizeTemp();
    }

    /**
     * @param int $tries
     * @return bool
     */
    private function removeOptimizeTemp($tries = 0)
    {
        set_error_handler(function(){});
        $remove = unlink($this->opFile());
        restore_error_handler();
        if (!$remove) {
            // 刚好其他进程再对临时文件进行读写, 多尝试几次
            if ($tries > 20) {
                return $this->setError('Remove optimize temp failed');
            }
            usleep(100000);
            return $this->removeOptimizeTemp(++$tries);
        }
        return true;
    }

    /**
     * 获取指定 hash index 链表索引 起始位置
     * @param $handler
     * @param $index
     * @return int
     */
    protected function getKeyPosition($handler, $index)
    {
        return $this->getUnpackInt($handler, $this->getIndexPosition($index));
    }

    /**
     * @param $handler
     * @param $seek
     * @return int
     */
    protected function getUnpackInt($handler, $seek)
    {
        fseek($handler, $seek, SEEK_SET);
        $position = unpack('V', fread($handler, 4));
        if (!is_array($position) || !isset($position[1])) {
            return $this->setError('Read pack number failed');
        }
        return (int) $position[1];
    }

    /**
     * @param $index
     * @return int
     */
    protected function getIndexPosition($index)
    {
        return $index * 4 + $this->getStartLength();
    }

    /**
     * header length: test/status/optimized/createTime/count
     * @return int
     */
    protected function getStartLength()
    {
        return $this->getHeaderLen(11);
    }

    /**
     * @param int $add
     * @return int
     */
    protected function getHeaderLen($add = 0)
    {
        return ($this->phpPath ? 13 : 0) + $add;
    }

    /**
     * @param $handler
     * @param $buffer
     * @param int $tries
     * @return bool
     */
    protected function writeBufferToHandler($handler, $buffer, $tries = 0)
    {
        if (!($len = strlen($buffer))) {
            return true;
        }
        $writeLen = fwrite($handler, $buffer);
        if ($writeLen === $len) {
            return true;
        }
        if ($tries > 99) {
            return $this->setError('Write buffer failed');
        }
        usleep(100);
        fseek($handler, -1 * $writeLen, SEEK_CUR);
        return $this->writeBufferToHandler($handler, $buffer, ++$tries);
    }

    /**
     * @return string
     */
    protected function opFile()
    {
        return $this->path. '.op';
    }

    /**
     * @return string
     */
    protected function lockFile()
    {
        return $this->path. '.lock';
    }

    /**
     * 获取当前存储键值的总数量
     * @return int
     */
    public function count()
    {
        return (int) ($handler = $this->getHandler(self::HANDLER_GET))
            ? $this->getUnpackInt($handler, $this->getHeaderLen(7)) : 0;
    }

    /**
     * 总数 +1
     * @param $handler
     * @return bool
     */
    protected function increaseCount($handler)
    {
        $seek = $this->getHeaderLen(7);
        if (false === ($position = $this->getUnpackInt($handler, $seek))) {
            return false;
        }
        $count = max(0,$position+ 1);
        fseek($handler, $seek);
        return $this->writeBufferToHandler($handler, pack('V', $count));
    }

    /**
     * 总数 -1
     * @param $handler
     * @return bool
     */
    protected function decreaseCount($handler)
    {
        $seek = $this->getHeaderLen(7);
        if (false === ($position = $this->getUnpackInt($handler, $seek))) {
            return false;
        }
        $count = max(0, $position - 1);
        fseek($handler, $seek);
        return $this->writeBufferToHandler($handler, pack('V', $count));
    }

    /**
     * 清除所有数据
     * @return bool
     */
    public function clear()
    {
        if (!($handler = $this->getHandler())) {
            return true;
        }
        if (!$this->writeClearStatus($handler)) {
            return false;
        }
        // 刚好在优化过程中, 清除旧缓存文件
        if (!($handler = $this->getOptimizeHandler())) {
            return true;
        }
        $clear = $this->writeClearStatus($handler);
        $this->closeOptimizeHandler(false);
        return $clear;
    }

    /**
     * 设置头部 status=1
     * @param $handler
     * @return bool
     */
    protected function writeClearStatus($handler)
    {
        if (!flock($handler, LOCK_EX)) {
            return false;
        }
        fseek($handler, $this->getHeaderLen(1), SEEK_SET);
        $clear = (bool) $this->writeBufferToHandler($handler, '1');
        flock($handler, LOCK_UN);
        return $clear;
    }

    /**
     * @return $this
     */
    public function close()
    {
        if ($this->handler) {
            if (is_resource($this->handler)) {
                fclose($this->handler);
            }
            $this->handler = null;
        }
        return $this;
    }

    /**
     * 测试用: 获取链表长度统计
     * @return array
     */
    public function testLinkedCount()
    {
        $handler = $this->getHandler();
        fseek($handler, $this->getStartLength(), SEEK_SET);
        $header = unpack('V*', fread($handler, self::MASK * 4));
        $header = array_filter($header);
        $header = array_values($header);
        $counts = [];
        foreach ($header as $key => $position) {
            $counts[$key] = $this->getIndexCount($handler, $position);
        }
        return array_count_values($counts);
    }

    /**
     * 测试用: 获取链表长度统计(子函数)
     * @param $handler
     * @param $position
     * @param int $count
     * @return int
     */
    private function getIndexCount($handler, $position, $count = 0)
    {
        $count++;
        fseek($handler, $position + 18, SEEK_SET);
        $header = unpack('V', fread($handler, 4));
        if (is_array($header) && isset($header[1]) && $header[1] > 0) {
            return $this->getIndexCount($handler, $header[1], $count);
        }
        return $count;
    }

    /**
     * close file handler
     */
    public function __destruct()
    {
        $this->close();
    }

    /**
     * get head position
     * 确定指定 key 值要放到哪个桶中
     * @param $key
     * @return int
     */
    public static function getKeyIndex($key)
    {
        $key = (string) $key;
        if (!strlen($key)) {
            return false;
        }
        return crc32($key) % self::MASK;
    }

    /**
     * returns the same int value on a 64 bit mc. like the crc32() function on a 32 bit mc.
     * @param $str
     * @return int
     */
    protected static function crc($str)
    {
        return hash('crc32', $str, true);
    }

    /**
     * 序列化函数  可以考虑 https://msgpack.org/
     * @param $value
     * @return string
     */
    protected static function serialize($value)
    {
        return serialize($value);
    }

    /**
     * 反序列化函数
     * @param $value
     * @return mixed
     */
    protected static function unSerialize($value)
    {
        return unserialize($value);
    }

    /**
     * 循环读取中 读取数据接口
     * 1. position 位置无法读取 header 信息, 返回 0
     * 2. position 位置获取到 header 信息 但未获取到值, 返回 header 中的 (int) next position 值
     * 3. position 位置正确获取 header 和 值, 返回数组 [next => int , key => string, value => mixed]
     * 4. 若 $onlyPosition=true 只需返回 position 位置 header 中的 (int) next position
     * 5. op=true 的情况是优化进程在读取, 可返回不同的 value, 用于 writeOptimize
     * @param $handler
     * @param $position
     * @param bool $op
     * @param bool $onlyPosition
     * @return array|int
     */
    abstract protected function readValueForIterator($handler, $position, $op = false, $onlyPosition = false);

    /**
     * 将从优化进程缓存文件中读取的数据 写入到 当前真实的缓存文件中
     * @param $handler
     * @param string $key  键值
     * @param int $index   key值的头部索引位置
     * @param mixed $value readValueForIterator() 函数 op=true 的返回值
     */
    abstract protected function writeOptimize($handler, $key, $index, $value);
}
