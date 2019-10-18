<?php
namespace Tanbolt\Fcache;

/**
 * Class Fcache
 * @package Tanbolt\Fcache
 * 使用文件系统实现 KV 键值存储数据库
 *
 * 数据结构 (头部索引结构参见 abstract Handler)
 * ------------------------------------------------------------------------------------------------------
 * 通过索引头部到指定 position 的数据结构
 * keyLength(2) | valueLength(4) | crc(4) | expire(4)| prePosition(4) | nxtPosition(4) - 22bit ( key  | value)
 * key长度      | 值长度         | 数据crc | 过期时间 | 前置key位置     | 后置key位置             ( key值 | 数据)
 *
 */
class Fcache extends Handler implements \ArrayAccess
{
    /**
     * 优化时候逐个读取
     * @var bool
     */
    protected $opOneByOne = true;

    /**
     * 设置键值对
     * @param string $key 键名
     * @param mixed $value 键值。字符串和整型被以原文存储，其他类型序列化后存储
     * @param int $expire 当前写入缓存的数据的失效时间，从当前时间算起以秒为单位的整数，0为永不过期
     * @return bool
     */
    public function set($key, $value, $expire = 0)
    {
        return $value === null ? $this->remove($key) : $this->doSetValue($key, $value, $expire);
    }

    /**
     * 设置计数, $increment 可为负值, 不存在则自动插入, 返回最终值
     * @param $key
     * @param int $increment
     * @param int $expire
     * @return int|false
     */
    public function increase($key, $increment = 1, $expire = 0)
    {
        return $this->doSetValue($key, intval($increment), $expire, true);
    }

    /**
     * @param $key
     * @param $value
     * @param $expire
     * @param bool $increase
     * @return bool
     */
    protected function doSetValue($key, $value, $expire, $increase = false)
    {
        if (false === ($index = static::getKeyIndex($key)) || !($handler = $this->getHandler(self::HANDLER_SET))) {
            return false;
        }
        return $this->setValue($handler, $index, $key, $value, $expire, false, $increase);
    }

    /**
     * @param $handler
     * @param $index
     * @param $key
     * @param $mixValue
     * @param int $expire
     * @param bool $optimize
     * @param bool $increase
     * @return bool|int
     */
    protected function setValue($handler, $index, $key, $mixValue, $expire = 0, $optimize = false, $increase = false)
    {
        if (!flock($handler, LOCK_EX) || false === ($position = $this->getKeyPosition($handler, $index))) {
            return false;
        }
        $bucketPosition = $this->getIndexPosition($index);

        // 当前链表还没启用 (对于 $increase 情况设置 16 个占位符)
        if ($position === 0) {
            $value = static::serialize($mixValue);
            fseek($handler, 0, SEEK_END);
            $headPosition = ftell($handler);
            if ($ok = $this->writeBufferToHandler(
                $handler,
                $this->packKeyValue($key, $value, $expire, 0, 0, $increase ? 16 : 0))
            ) {
                fseek($handler, $bucketPosition, SEEK_SET);
                $this->writeBufferToHandler($handler, pack('L', $headPosition));
                $this->increaseCount($handler);
            }
            flock($handler, LOCK_UN);
            return $ok && $increase ? $mixValue : (bool) $ok;
        }

        // 链表已启用 获取当前key在链表中位置, 若不存在, 会返回链表最后一个元素的位置信息
        // header: kLen(2) | eLen(4) | vLen(4) | crc(4) | expire(4) | prev(4) | next(4)
        if (!($header = $this->getKeyHeaderByPosition($handler, $key, $position))) {
            flock($handler, LOCK_UN);
            return false;
        }

        // 是优化进程来写入旧值的, 当前已存在, 说明在优化期间, 重新设置过, 所以忽略优化进行的写操作
        if ($optimize && $header['current']) {
            flock($handler, LOCK_UN);
            return true;
        }

        // 该 key 值不存在, 插入到链表开头
        // 后添加的 key 一般在业务中更可能会被读取, 这样就可减少读取时的时间复杂度
        if (!$header['current']) {
            $value = static::serialize($mixValue);
            fseek($handler, 0, SEEK_END);
            $headPosition = pack('L', ftell($handler));
            if ($ok = $this->writeBufferToHandler(
                $handler,
                $this->packKeyValue($key, $value, $expire, 0, $position, $increase ? 16 : 0))
            ) {
                fseek($handler, $bucketPosition, SEEK_SET);
                $this->writeBufferToHandler($handler, $headPosition);

                fseek($handler, $position + 18, SEEK_SET);
                $this->writeBufferToHandler($handler, $headPosition);
                $this->increaseCount($handler);
            }
            flock($handler, LOCK_UN);
            return $ok && $increase ? $mixValue : (bool) $ok;
        }

        // 自增数字 首选读取当前值, 进而设置写入值
        if ($increase) {
            $nowValue = $this->getValueByHeader($handler, $header);
            $mixValue += intval($nowValue);
        }
        $value = static::serialize($mixValue);
        if (strlen($value) <= $header['eLen']) {
            // new value length is smaller
            fseek($handler, $header['position'], SEEK_SET);
            $ok = $this->writeBufferToHandler(
                $handler,
                $this->packKeyValue($key, $value, $expire, $header['prev'], $header['next'], $header['eLen'], true)
            );
        } else {
            // new value length is bigger
            fseek($handler, 0, SEEK_END);
            $headPosition = pack('L', ftell($handler));
            if ($ok = $this->writeBufferToHandler(
                $handler,
                $this->packKeyValue($key, $value, $expire, $header['prev'], $header['next']))
            ) {
                // 修改前后 key 值的 prev next position
                fseek(
                    $handler,
                    $header['prev'] > 0 ? (int) $header['prev'] + 22 : $bucketPosition,
                    SEEK_SET
                );
                $this->writeBufferToHandler($handler, $headPosition);
                if ($header['next'] > 0) {
                    fseek($handler, (int) $header['next'] + 18, SEEK_SET);
                    $this->writeBufferToHandler($handler, $headPosition);
                }
                // 标记旧位置为无效状态
                fseek($handler, $header['position'], SEEK_SET);
                $this->writeBufferToHandler($handler, pack('S', 0));
            }
        }
        flock($handler, LOCK_UN);
        return $ok && $increase ? $mixValue : (bool) $ok;
    }

    /**
     * 设置键值的过期时间
     * @param string $key
     * @param int $expire 从当前时间算起以秒为单位的整数, 0为永不过期, -1为立即过期, >0当前时间后多少秒过期
     * @return bool
     */
    public function expire($key, $expire = -1)
    {
        if (false === ($index = static::getKeyIndex($key)) || !($handler = $this->getHandler(self::HANDLER_GET))) {
            return false;
        }
        if ($this->setExpire($handler, $index, $key, $expire)) {
            return true;
        }
        // 刚好在优化过程中, 且不存在 key, 尝试设置优化过程中的旧文件缓存
        if (!($handler = $this->getOptimizeHandler())) {
            return true;
        }
        return $this->setExpire($handler, $index, $key, $expire);
    }

    /**
     * @param $handler
     * @param $index
     * @param $key
     * @param $expire
     * @return bool
     */
    protected function setExpire($handler, $index, $key, $expire)
    {
        if (!flock($handler, LOCK_EX)) {
            return false;
        }
        if (false === ($position = $this->getKeyPosition($handler, $index))) {
            flock($handler, LOCK_UN);
            return false;
        }
        // has no key
        if (!($header = $this->getKeyHeaderByPosition($handler, $key, $position)) || !$header['current']) {
            flock($handler, LOCK_UN);
            return false;
        }
        $expire = intval($expire);
        $expire = $expire > 0 ? time() + $expire : ($expire < 0 ? time() - 1 : 0);
        if ($expire === (int) $header['expire']) {
            flock($handler, LOCK_UN);
            return true;
        }
        fseek($handler, $header['position'] + 14, SEEK_SET);
        $ok = $this->writeBufferToHandler($handler, pack('L', $expire));
        flock($handler, LOCK_UN);
        return (bool) $ok;
    }

    /**
     * 移除指定键名的值
     * @param string $key
     * @return bool
     */
    public function remove($key)
    {
        if (false === ($index = static::getKeyIndex($key)) || !($handler = $this->getHandler(self::HANDLER_GET))) {
            return false;
        }
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
        if(false === ($position = $this->getKeyPosition($handler, $index)) ||
            !($header = $this->getKeyHeaderByPosition($handler, $key, $position)) ||
            !$header['current']
        ) {
            flock($handler, LOCK_UN);
            return true;
        }
        // 修改当前值的 kLen, 标记为无效
        fseek($handler, $header['position'], SEEK_SET);
        if ($ok = $this->writeBufferToHandler($handler, pack('S', 0))) {
            // 修改前一个 key 值的 next position
            fseek(
                $handler,
                $header['prev'] > 0 ? (int) $header['prev'] + 22 : $this->getIndexPosition($index),
                SEEK_SET
            );
            $this->writeBufferToHandler($handler, pack('L', $header['next']));

            // 修改后一个 key 值的 prev position
            if ($header['next'] > 0) {
                fseek($handler, (int) $header['next'] + 18, SEEK_SET);
                $this->writeBufferToHandler($handler, pack('L', $header['prev']));
            }
            $this->decreaseCount($handler);
        }
        flock($handler, LOCK_UN);
        return (bool) $ok;
    }

    /**
     * 获取指定键名的存储值
     * @param string $key 键名
     * @return mixed
     */
    public function get($key)
    {
        return $this->getKey($key);
    }

    /**
     * 获取指定键名的可存活时长
     * @param string $key 键名
     * @return int|null  从当前时间算起以秒为单位的整数 (false:不存在, -1:永不过期, 0:已过期, int:可存活秒数)
     */
    public function ttl($key)
    {
        if (null === ($ttl = $this->getKey($key, true))) {
            return false;
        }
        return (int) $ttl === 0 ? -1 : max(0, (int) ($ttl - time()));
    }

    /**
     * 当前缓存文件若未获取到 && 正在优化 -> 从优化文件中尝试获取
     * @param $key
     * @param bool $getTtl
     * @return mixed|null
     */
    protected function getKey($key, $getTtl = false)
    {
        if (false === ($index = static::getKeyIndex($key)) || !($handler = $this->getHandler(self::HANDLER_GET))) {
            return null;
        }
        if (null !== ($value = $this->getValue($handler, $index, $key, $getTtl))) {
            return $value;
        }
        // 刚好在优化过程中, 尝试从旧文件缓存获取
        if (!($handler = $this->getOptimizeHandler())) {
            return null;
        }
        return $this->getValue($handler, $index, $key, $getTtl);
    }

    /**
     * 读取无需加锁, fcache 应该作为缓存使用, 应允许一定的脏数据, 其实概率极小
     * @param $handler
     * @param $index
     * @param $key
     * @param bool $getTtl
     * @return bool|mixed|null|string
     */
    protected function getValue($handler, $index, $key, $getTtl = false)
    {
        if (!$handler || false === ($position = $this->getKeyPosition($handler, $index)) ||
            !($header = $this->getKeyHeaderByPosition($handler, $key, $position)) ||
            !$header['current']
        ) {
            return null;
        }
        return $this->getValueByHeader($handler, $header, $getTtl);
    }

    /**
     * 通过 Header 读取数据
     * @param $handler
     * @param $header
     * @param bool $getTtl
     * @return bool|mixed|string|null
     */
    protected function getValueByHeader($handler, $header, $getTtl = false)
    {
        fseek($handler, (int) $header['position'] + (int) $header['kLen'] + 26, SEEK_SET);
        $data = fread($handler, $header['vLen']);
        if ($getTtl) {
            $data = static::crc($data) !== $header['crc'] ? null : $header['expire'];
        } else {
            $data = $this->checkValue($data, $header);
        }
        return $data;
    }

    /**
     * @param $data
     * @param $header
     * @return mixed|null
     */
    protected function checkValue($data, $header)
    {
        if (static::crc($data) !== $header['crc']) {
            return null;
        }
        if ($header['expire'] > 0 && $header['expire'] < time()) {
            return null;
        }
        return static::unserialize($data);
    }

    /**
     * 从链表开始位置 获取指定 key 的头部信息
     * @param $handler
     * @param $key
     * @param $position
     * @return array|bool
     */
    protected function getKeyHeaderByPosition($handler, $key, $position)
    {
        $linkCount = 0;
        $header = false;
        $key = (string) $key;
        while ($position > 0 && (!self::LINK || $linkCount < self::LINK)) {
            if (!($keyHeader = $this->getHeaderByPosition($handler, $position))) {
                break;
            }
            $header = $keyHeader;
            $header['current'] = false;
            if (!$header['kLen'] || fread($handler, (int) $header['kLen']) !== $key) {
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
        $header = fread($handler, 26);
        $header = strlen($header) === 26 ? unpack('SkLen/LeLen/LvLen/lcrc/Lexpire/Lprev/Lnext', $header) : null;
        if (!is_array($header) || count($header) !== 7) {
            $this->setError('Get key header failed');
            return false;
        }
        $header['position'] = $position;
        return $header;
    }

    /**
     * 打包键值对
     * @param $key
     * @param $value
     * @param $expire
     * @param $prePosition
     * @param $nxtPosition
     * @param int $expectLen
     * @param bool $reWrite
     * @return string
     */
    protected function packKeyValue($key, $value, $expire, $prePosition, $nxtPosition, $expectLen = 0, $reWrite = false)
    {
        $crc = static::crc($value);
        $expire = $expire ? time() + $expire : 0;

        // 若 value 长度较为固定, 首次写入可设置一个期望长度值, 这样修改时会在原位置进行修改
        $eLen = $vLen = strlen($value);
        if ($expectLen && $expectLen > $vLen) {
            $eLen = $expectLen;
            if (!$reWrite) {
                $value .= str_repeat('0', $eLen - $vLen);
            }
        }
        // keyLength(2) | expectLen(4) | valueLength(4) | crc(4) | expire(4) | prePosition(4) | nxtPosition(4)
        $header = pack('SLLlLLL', strlen($key), $eLen, $vLen, $crc, $expire, $prePosition, $nxtPosition);
        return $header.$key.$value;
    }

    /**
     * @param mixed $offset
     * @return bool
     */
    public function offsetExists($offset)
    {
        return $this->get($offset) !== null;
    }

    /**
     * @param mixed $offset
     * @return array|null
     */
    public function offsetGet($offset)
    {
        return $this->get($offset);
    }

    /**
     * @param mixed $offset
     * @param mixed $value
     */
    public function offsetSet($offset, $value)
    {
        $this->set($offset, $value);
    }

    /**
     * @param mixed $offset
     */
    public function offsetUnset($offset)
    {
        $this->remove($offset);
    }

    /**
     * 循环读取执行函数
     * 1. position 位置无法读取 header 信息, 返回 0
     * 2. position 位置获取到 header 信息 但未获取到值, 返回 header 中的 next position 值
     * 3. position 位置正确获取 header 和 值, 返回数组 [next => int , key => string, value => mixed]
     * 4. 若 $onlyPosition=true 只需返回 position 位置 header 中的 next
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
        $key = $value = false;
        if (!$onlyPosition && $header['kLen'] && $header['vLen']) {
            fseek($handler, $header['position'] + 26, SEEK_SET);
            $key = fread($handler, $header['kLen']);
            $val = $this->checkValue(fread($handler, $header['vLen']), $header);
            if (strlen($key) && $val !== null) {
                $value = $op ? [$val, $header['expire']] : $val;
            }
        }
        if ($value === false) {
            return $next;
        }
        return [
            'next' => $next,
            'key' => $key,
            'value' => $value
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
        list($value, $expire) = $value;
        $this->setValue($handler, $index, $key, $value, ($expire ? $expire - time() : 0), true);
    }
}
