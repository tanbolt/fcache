<?php
namespace Tanbolt\Fcache;

/**
 * Class Fkey
 * @package Tanbolt\Fcache
 * 使用文件系统实现 KK 键值存储数据库
 * 即简单的  添加:fkey->add(key), 判断:fkey->has(key)
 * 针对此类需求更常使用的是 布隆过滤器, 但文件版的实测性能并不是很高, fkey 可以作为一个替代品, 且拥有了无误判率的优势
 *
 * 数据结构 (头部索引结构参见 abstract Handler)
 * ------------------------------------------------------------------------------------------------------
 * 通过索引头部到指定 position 的数据结构
 * prev(4)     / next(4)     / key(16)
 * 前置key位置  / 后置key位置  / key 的 md5 raw
 */
class Fkey extends Handler implements \ArrayAccess
{
    /**
     * 新增一个 key
     * @param $key
     * @return bool
     */
    public function add($key)
    {
        if (false === ($index = static::getKeyIndex($key)) || !($handler = $this->getHandler(self::HANDLER_SET))) {
            return false;
        }
        return $this->addKey($handler, $index, md5($key, true));
    }

    /**
     * 写入新增 key
     * @param $handler
     * @param $index
     * @param $key
     * @return bool
     */
    protected function addKey($handler, $index, $key)
    {
        $bucketPosition = $this->getIndexPosition($index);
        if (!flock($handler, LOCK_EX) || false === ($position = $this->getUnpackInt($handler, $bucketPosition))) {
            return false;
        }

        // 当前链表还没启用
        if ($position === 0) {
            fseek($handler, 0, SEEK_END);
            $headPosition = ftell($handler);
            if ($ok = $this->writeBufferToHandler(
                $handler,
                pack('VV', 0, 0).$key
            )) {
                fseek($handler, $bucketPosition, SEEK_SET);
                $this->writeBufferToHandler($handler, pack('V', $headPosition));
                $this->increaseCount($handler);
            }
            flock($handler, LOCK_UN);
            return (bool) $ok;
        }

        // 链表已启用 获取当前key在链表中位置, 若不存在, 会返回链表最后一个元素的位置信息, 发送错误或已存在, 直接返回
        // header:prev(4) / next(4) / md5(16)
        if (!($header = $this->getKeyHeaderByPosition($handler, $key, $position)) || $header['current']) {
            flock($handler, LOCK_UN);
            return (bool) $header;
        }

        // 不存在, 插入到链表开头
        // 后添加的 key 一般在业务中更可能会被读取, 这样就可减少读取时的时间复杂度
        fseek($handler, 0, SEEK_END);
        $headPosition = pack('V', ftell($handler));
        if ($ok = $this->writeBufferToHandler(
            $handler,
            pack('VV', 0, $position).$key
        )) {
            // 修改桶中 position
            fseek($handler, $bucketPosition, SEEK_SET);
            $this->writeBufferToHandler($handler, $headPosition);

            // 修改链表开头元素的 prev position
            fseek($handler, $position, SEEK_SET);
            $this->writeBufferToHandler($handler, $headPosition);
            $this->increaseCount($handler);
        }
        flock($handler, LOCK_UN);
        return (bool) $ok;
    }

    /**
     * 移除一个 key
     * @param $key
     * @return bool
     */
    public function remove($key)
    {
        if (false === ($index = static::getKeyIndex($key)) || !($handler = $this->getHandler(self::HANDLER_GET))) {
            return false;
        }
        $key = md5($key, true);
        if (!$this->removeValue($handler, $index, $key)) {
            return false;
        }
        // 刚好在优化过程中, 尝试删除旧文件缓存的 key
        if (!($handler = $this->getOptimizeHandler())) {
            return true;
        }
        return $this->removeValue($handler, $index, $key);
    }

    /**
     * 修改 Prev Next 值将 kv 从链表中移除
     * @param $handler
     * @param $index
     * @param $key
     * @return bool
     */
    protected function removeValue($handler, $index, $key)
    {
        if (!flock($handler, LOCK_EX)) {
            return false;
        }
        // not has key
        $bucketPosition = $this->getIndexPosition($index);
        if(false === ($position = $this->getUnpackInt($handler, $bucketPosition)) ||
            !($header = $this->getKeyHeaderByPosition($handler, $key, $position)) ||
            !$header['current']
        ) {
            flock($handler, LOCK_UN);
            return true;
        }
        // 修改前一个元素 的 next position
        fseek(
            $handler,
            $header['prev'] > 0 ? (int) $header['prev'] : $bucketPosition,
            SEEK_SET
        );
        if ($ok = $this->writeBufferToHandler($handler, pack('V', $header['next']))) {
            if ($header['next'] > 0) {
                // 修改后一个元素 的 prev position
                fseek($handler, (int) $header['next'] + 4, SEEK_SET);
                $ok = $this->writeBufferToHandler($handler, pack('V', $header['prev']));
            }
            $this->decreaseCount($handler);
        }
        flock($handler, LOCK_UN);
        return (bool) $ok;
    }

    /**
     * key 是否已存在
     * @param $key
     * @return bool
     */
    public function has($key)
    {
        return (bool) $this->get($key);
    }

    /**
     * 获取 key 是否存在, 返回 key 的 md5 binary
     * @param $key
     * @return bool|mixed|string|null
     */
    protected function get($key)
    {
        if (false === ($index = static::getKeyIndex($key)) || !($handler = $this->getHandler(self::HANDLER_GET))) {
            return null;
        }
        $key = md5($key, true);
        if (null !== ($value = $this->getValue($handler, $index, $key))) {
            return $value;
        }
        // 刚好在优化过程中, 尝试从旧文件缓存获取
        if (!($handler = $this->getOptimizeHandler())) {
            return null;
        }
        return $this->getValue($handler, $index, $key);
    }

    /**
     * 读取无需加锁
     * @param $handler
     * @param $index
     * @param $key
     * @return bool|mixed|null|string
     */
    protected function getValue($handler, $index, $key)
    {
        if (!$handler || false === ($position = $this->getKeyPosition($handler, $index)) ||
            !($header = $this->getKeyHeaderByPosition($handler, $key, $position)) ||
            !$header['current']
        ) {
            return null;
        }
        return $key;
    }

    /**
     * 从链表开始位置 获取指定 key 信息
     * @param $handler
     * @param $key
     * @param $position
     * @return array|bool|false
     */
    protected function getKeyHeaderByPosition($handler, $key, $position)
    {
        $linkCount = 0;
        $header = false;
        while ($position > 0 && (!self::LINK || $linkCount < self::LINK)) {
            if (!($keyHeader = $this->getHeaderByPosition($handler, $position))) {
                break;
            }
            $header = $keyHeader;
            $header['current'] = false;
            if (fread($handler, 16) !== $key) {
                $position = $header['next'];
                $linkCount++;
                continue;
            }
            $header['current'] = true;
            return $header;
        }
        return $header;
    }

    /**
     * @param $handler
     * @param $position
     * @return array|false
     */
    protected function getHeaderByPosition($handler, $position)
    {
        fseek($handler, $position, SEEK_SET);
        $header = fread($handler, 8);
        $header = strlen($header) === 8 ? unpack('Vprev/Vnext', $header) : null;
        if (!$header || count($header) !== 2) {
            return $this->setError('Get key header failed');
        }
        $header['position'] = $position;
        return $header;
    }

    /**
     * @param mixed $offset
     * @return bool
     */
    public function offsetExists($offset)
    {
        return $this->has($offset);
    }

    /**
     * @param mixed $offset
     * @return array|null
     */
    public function offsetGet($offset)
    {
        return bin2hex($this->get($offset));
    }

    /**
     * @param mixed $offset
     * @param mixed $value
     */
    public function offsetSet($offset, $value)
    {
        if ((bool) $value) {
            $this->add($offset);
        } else {
            $this->remove($offset);
        }
    }

    /**
     * @param mixed $offset
     */
    public function offsetUnset($offset)
    {
        $this->remove($offset);
    }

    /**
     * 这里的优化 只是为了完成 Handler 的 abstract, 对于 Fkey 结构, 无需优化, 不会产生冗长数据
     * 循环读取执行函数
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
    protected function readValueForIterator($handler, $position, $op = false, $onlyPosition = false)
    {
        $header = $this->getHeaderByPosition($handler, $position);
        if (!is_array($header)) {
            return 0;
        }
        $next = ($next = (int) $header['next']) === $position ? 0 : $next;
        $key = false;
        if (!$onlyPosition) {
            fseek($handler, $header['position'] + 8, SEEK_SET);
            $key = fread($handler, 16);
            if (strlen($key) === 16) {
                $key = $op ? $key : bin2hex($key);
            }
        }
        if ($key === false) {
            return $next;
        }
        return [
            'next' => $next,
            'key' => $key,
            'value' => 1
        ];
    }

    /**
     * 将从优化进程缓存文件中读取的数据 写入到 当前真实的缓存文件中
     * @param $handler
     * @param string $key  键值
     * @param int $index   key值的头部索引位置
     * @param mixed $value readValueForIterator() 函数 op=true 的返回值
     */
    protected function writeOptimize($handler, $key, $index, $value)
    {
        $this->addKey($handler, $index, $key);
    }
}
