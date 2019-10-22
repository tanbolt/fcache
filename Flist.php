<?php
namespace Tanbolt\Fcache;
use RuntimeException;

/**
 * Class Flist
 * @package Tanbolt\Fcache
 * 使用文件系统实现的类似 redis LIST 数据缓存系统
 *
 * 数据结构 (头部索引结构参见 abstract Handler)
 * ------------------------------------------------------------------------------------------------------
 * 通过索引头部到指定 position 的数据结构
 * keyLength(2) / prevPosition(4) / nextPosition(4) / valuePosition(4)  - keyValue
 * key长度      / 前置key位置      /  后置key位置     /  keyLists 起始位置  - key 值
 * ------------------------------------------------------------------------------------------------------
 * 通过key索引到指定的 position 数据结构
 * valueLength(4) / prevPosition  / nextPosition / valueCrc(4) -  value
 * key长度        / 前一个数据位置  / 后一个数据位置  / 数据crc     -  value数据
 *
 */
class Flist extends Handler implements \ArrayAccess
{
    /**
     * @var array
     */
    private $values = [];

    /**
     * 添加一个待存储值
     * @param mixed $value 待添加的数据
     * @param bool $prepend 是否添加到开头，默认追加到尾部
     * @return $this
     */
    public function addValue($value, $prepend = false)
    {
        if ($prepend) {
            array_unshift($this->values, $value);
        } else {
            $this->values[] = $value;
        }
        return $this;
    }

    /**
     * 移除一个待添加值
     * @param mixed $value 要移除的值
     * @param bool $all 是否移除所有匹配的值，默认仅移除第一个匹配项
     * @param bool $strict 判断在搜索的时候是否该使用严格的比较（===）
     * @return $this
     */
    public function removeValue($value, $all = false, $strict = false)
    {
        $keys = $all ? array_keys($this->values, $value, $strict) : [array_search($value, $this->values, $strict)];
        foreach ($keys as $key) {
            if ($key === false) {
                continue;
            }
            unset($this->values[$key]);
        }
        $this->values = array_values($this->values);
        return $this;
    }

    /**
     * 重置所有待存储值
     * @param $value
     * @return $this
     */
    public function setValue(array $value)
    {
        $this->values = $value;
        return $this;
    }

    /**
     * 清除所有待添加值
     * @return $this
     */
    public function clearValue()
    {
        $this->values = [];
        return $this;
    }

    /**
     * 将已设置的待添加值追加到 $key 的尾部, 如果 $key 不存在, 创建一个
     * @param string $key  键名
     * @return bool
     */
    public function push($key)
    {
        return $this->writeKeyCurrent($key, 'eof', true, true);
    }

    /**
     * 将已设置的待添加值添加到 $key 的头部, 如果 $key 不存在, 创建一个
     * @param string $key 键名
     * @return bool
     */
    public function insert($key)
    {
        return $this->writeKeyCurrent($key, 'eof', false, true);
    }

    /**
     * 将已设置的待添加值追加到键名为 $key 的 $pivot 值之后，若 $key 或 $pivot 不存在，添加失败
     * @param string $key 键名
     * @param mixed $pivot 判断值
     * @return bool
     */
    public function append($key, $pivot)
    {
        return $this->writeKeyCurrent($key, $pivot, true, false);
    }

    /**
     * 将已设置的待添加值添加到键名为 $key 的 $pivot 值之前，若 $key 或 $pivot 不存在，添加失败
     * @param string $key 键名
     * @param mixed $pivot 判断值
     * @return bool
     */
    public function prepend($key, $pivot)
    {
        return $this->writeKeyCurrent($key, $pivot, false, false);
    }

    /**
     * 将已设置的待添加值追加到键名为 $key 的 $index 下标之后，若 $key 或 $index 不存在，添加失败
     * $index>=0: 在指定的下标 $index 之后 插入值
     * $index<0:  在从结尾往前数 $index 的值之后 插入值
     * @param string $key 键名
     * @param int|bool $index 下标
     * @return bool
     */
    public function appendByIndex($key, $index = -1)
    {
        return $this->writeKeyCurrent($key, $index, true, true);
    }

    /**
     * 将已设置的待添加值追添加键名为 $key 的 $index 下标之前，若 $key 或 $index 不存在，添加失败
     * $index>=0: 在指定的下标 $index 之前 插入值
     * $index<0:  在从结尾往前数 $index 的值之前 插入值
     * @param string $key 键名
     * @param int $index 下标
     * @return bool
     */
    public function prependByIndex($key, $index = 0)
    {
        return $this->writeKeyCurrent($key, $index, false, true);
    }

    /**
     * 给指定的 key 写入 $this->values 数据
     * @param $key
     * @param $pivot
     * @param bool $append
     * @param bool $isIndex
     * @return bool
     */
    protected function writeKeyCurrent($key, $pivot, $append = true, $isIndex = false)
    {
        $type = $isIndex && $pivot === 'eof' ? self::HANDLER_SET : self::HANDLER_GET;
        if (false === ($index = static::getKeyIndex($key))
            || empty($this->values)
            || !($handler = $this->getHandler($type))
        ) {
            return false;
        }
        return $this->writeKey($handler, $key, $index, $this->values, $pivot, $append, $isIndex, true, false);
    }

    /**
     * 写入指定 key 的 list 的数据 (先根据情况加锁)
     * @param $handler
     * @param $key
     * @param $index
     * @param $values
     * @param $pivot
     * @param bool $append
     * @param bool $isIndex
     * @param bool $lock 是否需要加锁
     * @param bool $op  是否为优化过程来写入数据
     * @return bool
     */
    protected function writeKey($handler, $key, $index, $values, $pivot, $append = true, $isIndex = false, $lock = false, $op = false)
    {
        if (!$values || ($lock && !flock($handler, LOCK_EX))) {
            return false;
        }
        try {
            $write = $this->writeKeyToFile($handler, $key, $index, $values, $pivot, $append, $isIndex, $op);
        } catch (RuntimeException $e) {
            $this->setError($e->getMessage());
            $write = false;
        }
        if ($lock) {
            flock($handler, LOCK_UN);
        }
        return (bool) $write;
    }

    /**
     * 写入指定 key 的 list 的数据
     * @param $handler
     * @param $key
     * @param $index
     * @param $values
     * @param $pivot
     * @param bool $append
     * @param bool $isIndex
     * @param bool $op
     * @return bool
     */
    protected function writeKeyToFile($handler, $key, $index, $values, $pivot, $append = true, $isIndex = false, $op = false)
    {
        $bucketPosition = $this->getIndexPosition($index);
        if (false === ($position = $this->getUnpackInt($handler, $bucketPosition))) {
            return false;
        }
        //keyHeader: [kLen, prev, next, position, now, key, current]
        $keyHeader = $this->getKeyHeader($handler, $position, $key, $op, false, null);

        // 桶里还未写入过数据
        if ($keyHeader === false) {
            if ($isIndex && $pivot === 'eof' && false !== ($headerPosition = $this->writeKeyHeader($handler, $key))) {
                $this->writePack($handler, $bucketPosition, $headerPosition)
                    ->writePack($handler, $headerPosition + 10,  $this->writeKeyValues($handler, $values, 0))
                    ->increaseCount($handler);
                return true;
            }
            return false;
        }

        // 桶里已有数据 但 (当前 key 不存在 || 存在但没有 value)
        if (!$keyHeader['current'] || !$keyHeader['position']) {
            if (!$isIndex || $pivot !== 'eof') {
                return false;
            }
            if ($keyHeader['current']) {
                // 有 key 但没有 value
                $headerPosition = (int) $keyHeader['now'];
            } else {
                // 没有 key, 插入桶链表的开头
                // 后添加的 key 一般在业务中更可能会被读取, 这样就可减少读取时的时间复杂度
                if (false === ($headerPosition = $this->writeKeyHeader($handler, $key, $position))) {
                    return false;
                }
                $this->writePack($handler, $bucketPosition, $headerPosition)
                    ->writePack($handler, $position + 2, $headerPosition)
                    ->increaseCount($handler);
            }
            $this->writePack($handler, $headerPosition + 10, $this->writeKeyValues($handler, $values, 0));
            return true;
        }

        // 优化进程前来写入数据, 如果已存在, 说明已从优化进程的临时文件提取过数据了, 忽略即可
        if ($op) {
            return true;
        }

        // 找到了 key 索引, 校验 pivot, 获取 value 插入点的位置信息
        if ($isIndex) {
            if ($pivot === 'eof') {
                $pivot = $append ? -1 : 0;
            } elseif (!is_int($pivot)) {
                return false;
            }
            $pivot = intval($pivot);
        } elseif (!strlen($pivot)) {
            return false;
        }

        $vHeader = $this->getValueHeaders($handler, $keyHeader['position'], $pivot, $isIndex, 1);
        if (!$vHeader) {
            return false;
        }
        $vHeader = reset($vHeader);

        // 写入数据到 插入点 的前后
        // valueLength/valueCrc/prevPosition/nextPosition - value
        $endPosition = 0;
        if ($append) {
            $position = $this->writeKeyValues($handler, $values, $vHeader['position'], $vHeader['next'], $endPosition);
            // 修改前一个 value 的 next position
            $this->writePack($handler, $vHeader['position'] + 8, $position);
            // 修改后一个 value 的 prev position
            if ($vHeader['next']) {
                $this->writePack($handler, $vHeader['next'] + 4, $endPosition);
            }
        } else {
            $prev = $vHeader['position'] === $keyHeader['position'] ? 0 : $vHeader['prev']; //是否插入到最开头
            $position = $this->writeKeyValues($handler, $values, $prev, $vHeader['position'], $endPosition);
            // 修改后一个 value 的 prev position
            $this->writePack($handler, $vHeader['position'] + 4, $endPosition);
            // 修改前一个 value 的 next position 或 key 中 value 起始 position
            if ($prev) {
                $this->writePack($handler, $prev + 8, $position);
            } else {
                $this->writePack($handler, $keyHeader['now'] + 10, $position);
            }
        }
        return true;
    }

    /**
     * 写入 key 的 header 数据
     * @param $handler
     * @param $key
     * @param int $next
     * @return bool|int
     */
    protected function writeKeyHeader($handler, $key, $next = 0)
    {
        fseek($handler, 0, SEEK_END);
        $position = ftell($handler);
        //keyLength/prevPosition/nextPosition/valuePosition - keyValue
        $header = pack('vVVV', strlen($key), 0, (int) $next, 0).$key;
        return $this->writeBufferToHandler($handler, $header) ? $position : false;
    }

    /**
     * 写入 key 的 list 数据
     * @param $handler
     * @param $values
     * @param $prev
     * @param int $nextPosition
     * @param int $endPosition
     * @return int
     */
    protected function writeKeyValues($handler, $values, $prev, $nextPosition = 0, &$endPosition = 0)
    {
        fseek($handler, 0, SEEK_END);
        $position = $endPosition = ftell($handler);
        $current = 1;
        $count = count($values);
        foreach ($values as $value) {
            $endPosition = ftell($handler);
            $self = $current < $count;
            $next = $self ? $endPosition : $nextPosition;
            $value = $this->packKeyValue($value, $prev, $next, $self);
            if ($this->writeBufferToHandler($handler, $value)) {
                $prev = $endPosition;
            } else {
                $this->writePack($handler, $endPosition + 8, 0);
                throw new RuntimeException('Write '.$current.'th value failed');
            }
            $current++;
        }
        return $position;
    }

    /**
     * 打包 key 的 list 数据
     * pack: vLen/crc/prev/next/value
     * @param $value
     * @param $prev
     * @param int $next
     * @param bool $self
     * @return string
     */
    protected function packKeyValue($value, $prev, $next = 0, $self = false)
    {
        $prev = (int) $prev;
        $value = static::serialize($value);
        $vLen = strlen($value);
        $next = (int) $next;
        if ($self) {
            $next = 16 + $next + $vLen;
        }
        // valueLength/prevPosition/nextPosition/valueCrc - value
        return pack('VVV', $vLen, $prev, $next).static::crc($value).$value;
    }

    /**
     * 修改键名 $key 下标为 $index 的 $value 值
     * @param string $key 键名
     * @param int $index 下标
     * @param mixed $value 添加值
     * @return bool
     */
    public function alter($key, $index, $value)
    {
        if (($index = intval($index)) < 0 || !($keyHeader = $this->getKeyHeaderByKey($key, $handler, true))) {
            return false;
        }
        if (!$keyHeader['position']) {
            flock($handler, LOCK_UN);
            return false;
        }
        $item = $this->getValueHeaders($handler, $keyHeader['position'], $index, true, 1);
        if (!$item) {
            flock($handler, LOCK_UN);
            return false;
        }
        $item = reset($item);
        $value = static::serialize($value);
        $vLen = strlen($value);
        $crc = static::crc($value);
        if ($crc === $item['crc']) {
            flock($handler, LOCK_UN);
            return true;
        }
        if ($vLen <= $item['vLen']) {
            // new value length is smaller
            fseek($handler, $item['position'], SEEK_SET);
            if ($save = $this->writeBufferToHandler($handler, pack('V', $vLen))) {
                fseek($handler, 8, SEEK_CUR);
                $save = $this->writeBufferToHandler($handler, $crc.$value);
            }
        } else {
            // new value length is bigger
            $pack = pack('VVV', $vLen, $item['prev'], $item['next']).$crc.$value;
            fseek($handler, 0, SEEK_END);
            $position = ftell($handler);
            $save = $this->writeBufferToHandler($handler, $pack);
            if ($save) {
                if ($item['next'] > 0) {
                    $this->writePack($handler, $item['next'] + 4, $position, false);
                }
                if ($item['prev'] > 0) {
                    $this->writePack($handler, $item['prev'] + 8, $position, false);
                } else {
                    $this->writePack($handler, $keyHeader['now'] + 10, $position, false);
                }
            }
        }
        flock($handler, LOCK_UN);
        return (bool) $save;
    }

    /**
     * 出栈，弹出数组最后一个单元
     * @param string $key 键名
     * @return mixed|bool 返回移出的值，不存在返回 false
     */
    public function pop($key)
    {
        if (!($keyHeader = $this->getKeyHeaderByKey($key, $handler, true))) {
            return false;
        }
        $value = $keyHeader['position'] ?
            $this->getValueHeaders($handler, $keyHeader['position'], -1, true, 1, true)
            : false;
        $pop = false;
        if ($value) {
            $value = reset($value);
            if ($this->writePack($handler, ($value['prev'] ? $value['prev'] + 8 : $keyHeader['now'] + 10), 0, false)) {
                $pop = $value['value'];
            }
        }
        flock($handler, LOCK_UN);
        return $pop;
    }

    /**
     * 出栈, 移除数组第一个单元
     * @param string $key 键名
     * @return mixed|bool 返回移出的值，不存在返回 false
     */
    public function shift($key)
    {
        if (!($keyHeader = $this->getKeyHeaderByKey($key, $handler, true))) {
            return false;
        }
        $value = $keyHeader['position'] ?
            $this->getValueHeaders($handler, $keyHeader['position'], 0, true, 1, true)
            : false;
        $shift = false;
        if ($value) {
            $value = reset($value);
            if ($this->writePack($handler, $keyHeader['now'] + 10, $value['next'], false)) {
                $shift = $value['value'];
            }
        }
        flock($handler, LOCK_UN);
        return $shift;
    }

    /**
     * 移除指定键名对应下标的值
     * @param string $key  键名
     * @param array|int $index 下标，可使用数组同时指定多个下标
     * @return bool
     */
    public function removeIndex($key, $index)
    {
        return $this->changeIndex($key, $index, false);
    }

    /**
     * 仅保留指定键名对应下标的值
     * @param string $key 键名
     * @param array|int $index 下标，可使用数组同时指定多个下标
     * @return bool
     */
    public function keepIndex($key, $index)
    {
        return $this->changeIndex($key, $index, true);
    }

    /**
     * (移除/保留) key 的指定下标的值
     * @param $key
     * @param $index
     * @param bool $keep
     * @return bool
     */
    protected function changeIndex($key, $index, $keep = false)
    {
        if (!is_array($index)) {
            $index = [$index];
        }
        $indexes = [];
        foreach ($index as $item) {
            if (is_scalar($item) && (ctype_digit(strval($item)))) {
                $indexes[] = intval($item);
            }
        }
        $indexes = array_unique($indexes);
        if (!count($indexes)) {
            return true;
        }
        if (!($keyHeader = $this->getKeyHeaderByKey($key, $handler, true))) {
            return false;
        }
        if (!$keyHeader['position']) {
            flock($handler, LOCK_UN);
            return false;
        }
        sort($indexes);
        $start = current($indexes);
        $end = end($indexes);
        $firstPosition = false;
        if ($keep) {
            // get old value
            $range = $this->getValueHeaders($handler, $keyHeader['position'], $start, true, $end-$start+1);
            // make new value
            $newRange = [];
            foreach ($range as $key => $value) {
                if (in_array($key, $indexes)) {
                    $newRange[] = $value;
                }
            }
            $range = $this->joinNewRange($newRange);
            $first = array_shift($range);
            if ($first && ($first['prev'] != 0 || !isset($first['newNext']) || $first['newNext'] != $first['next'])) {
                $first['newPrev'] = 0;
                array_unshift($range, $first);
            }
            $last = array_pop($range);
            if ($last && ($last['next'] != 0 || $last['newPrev'] != $last['prev'])) {
                $last['newNext'] = 0;
                $range[] = $last;
            }
            $firstPosition = count($range) ? current($range)['position'] : 0;
        } else {
            // get old value (在要移除 index 两端各扩展 1, 得到 range 后丢弃要移除的 index, 将剩余保留 index header 重写)
            $first = max(0, $start - 1);
            $length = $end - $first + 2;
            $range = $this->getValueHeaders($handler, $keyHeader['position'], $first, true, $length);
            // make new value
            $hasLastValue = count($range) >= $length;
            foreach ($indexes as $index) {
                if (isset($range[$index])) {
                    unset($range[$index]);
                }
            }
            if ($range) {
                $range = $this->joinNewRange($range);
                $first = array_shift($range);
                if ($first) {
                    if ($start === 0) {
                        $first['newPrev'] = 0;
                        $firstPosition = $first['position'];
                    } else {
                        $first['newPrev'] = $first['prev'];
                    }
                    array_unshift($range, $first);
                }
                $last = array_pop($range);
                if ($last) {
                    $last['newNext'] = $hasLastValue ? $last['next'] : 0;
                    $range[] = $last;
                }
            } else {
                $firstPosition = 0;
            }
        }
        if ($firstPosition !== false && $firstPosition != $keyHeader['position']) {
            fseek($handler, $keyHeader['now'] + 10, SEEK_SET);
            $this->writeBufferToHandler($handler, pack('V', $first['position']));
        }
        if (count($range)) {
            foreach ($range as $item) {
                fseek($handler, $item['position'] + 4, SEEK_SET);
                $this->writeBufferToHandler($handler, pack('VV', $item['newPrev'], $item['newNext']));
            }
        }
        flock($handler, LOCK_UN);
        return true;
    }

    /**
     * 拼接新的 list value 数据, 可一次性写入
     * @param $newRange
     * @return array
     */
    protected function joinNewRange($newRange)
    {
        if (!$newRange) {
            return [];
        }
        $range = [];
        while ($pop = array_shift($newRange)) {
            $prev = array_pop($range);
            if ($prev) {
                $pop['newPrev'] = $prev['position'];
                $prev['newNext'] = $pop['position'];
                $range[] = $prev;
            }
            $range[] = $pop;
        }
        $newRange = [];
        foreach ($range as $item) {
            if (isset($item['newPrev']) && $item['newPrev'] == $item['prev'] &&
                isset($item['newNext']) && $item['newNext'] == $item['next']
            ) {
                continue;
            }
            $newRange[] = $item;
        }
        return $newRange;
    }

    /**
     * 移除指定键名的部分值
     * @param string $key  键名
     * @param int $startIndex  移除值开始的下标(包含)
     * @param int|null $length 要移除值的长度,不指定则移除指定下标之后的所有值
     * @return bool
     */
    public function remove($key, $startIndex = 0, $length = null)
    {
        if (!is_int($startIndex) || !($keyHeader = $this->getKeyHeaderByKey($key, $handler, true))) {
            return false;
        }
        if (!$keyHeader['position']) {
            flock($handler, LOCK_UN);
            return false;
        }
        // remove all
        if ($startIndex === 0 && $length === null) {
            $ok = $this->writePack($handler, $keyHeader['now'] + 10, 0, false);
            flock($handler, LOCK_UN);
            return (bool) $ok;
        }
        $save = false;
        $range = $this->getValueHeaders($handler, $keyHeader['position'], $startIndex, true, $length);
        if ($range) {
            $first = array_shift($range);
            $last = array_pop($range);
            $next = $last ? $last['next'] : $first['next'];
            $save = $this->writePack($handler, ($first['prev'] ? $first['prev'] + 8: $keyHeader['now'] + 10), $next, false);
            if ($save && $next) {
                $save = $this->writePack($handler, $next + 4, $first['prev'], false);
            }
        }
        flock($handler, LOCK_UN);
        return (bool) $save;
    }

    /**
     * 仅保留指定键名的部分值
     * @param string $key 键名
     * @param int $startIndex 保留值开始的下标(包含)
     * @param null $length 要移除值的长度,不指定则保留指定下标之后的所有值
     * @return bool
     */
    public function keep($key, $startIndex = 0, $length = null)
    {
        if (!is_int($startIndex) || !($keyHeader = $this->getKeyHeaderByKey($key, $handler, true))) {
            return false;
        }
        if (!$keyHeader['position']) {
            flock($handler, LOCK_UN);
            return false;
        }
        $save = false;
        $range = $this->getValueHeaders($handler, $keyHeader['position'], $startIndex, true, $length);
        if ($range) {
            $first = array_shift($range);
            if ($this->writePack($handler, $keyHeader['now'] + 10, $first['position'], false)) {
                $last = array_pop($range) ?: $first;
                $save = $last['next'] ? $this->writePack($handler, $last['position'] + 8, 0, false) : true;
            }
        }
        flock($handler, LOCK_UN);
        return (bool) $save;
    }

    /**
     * 完全移除数据，包括键名
     * @param string $key 键名
     * @return bool
     */
    public function drop($key)
    {
        $ok = false;
        if ($keyHeader = $this->getKeyHeaderByKey($key, $handler, true)) {
            // 修改当前值的 kLen, 标记为无效
            fseek($handler, $keyHeader['now'], SEEK_SET);
            if ($this->writeBufferToHandler($handler, pack('v', 0))) {
                // 修改前一个 key 值的 next position
                if ($keyHeader['prev']) {
                    $this->writePack($handler, $keyHeader['prev'] + 6, $keyHeader['next'], false);
                } else {
                    $this->writePack($handler, $this->getIndexPosition(static::getKeyIndex($key)), $keyHeader['next'], false);
                }
                // 修改后一个 key 值的 prev position
                if ($keyHeader['next']) {
                    $this->writePack($handler, $keyHeader['next'] + 2, $keyHeader['prev'], false);
                }
                $this->decreaseCount($handler);
                $ok = true;
            }
            flock($handler, LOCK_UN);
        }
        // 若刚好正在优化, 同时标记优化文件中的 $key 为删除状态
        if (($handler = $this->getOptimizeHandler()) &&
            false !== ($index = static::getKeyIndex($key)) &&
            ($keyHeader = $this->getKeyHeader($handler, $this->getKeyPosition($handler, $index), $key, true, false, true))
        ) {
            fseek($handler, $keyHeader['now'], SEEK_SET);
            $this->writeBufferToHandler($handler, pack('v', 0));
        }
        return $ok;
    }

    /**
     * 判断是否存在指定键名
     * @param string $key
     * @return bool
     */
    public function exist($key)
    {
        return (bool) $this->getKeyHeaderByKey($key);
    }

    /**
     * 获取指定键名的数据长度
     * @param string $key 键名
     * @return int|bool  不存在返回 false，否则返回整数
     */
    public function len($key)
    {
        if (!($keyHeader = $this->getKeyHeaderByKey($key, $handler)) || !$keyHeader['position']) {
            return false;
        }
        return count($this->getValueHeaders($handler, $keyHeader['position']));
    }

    /**
     * 获取指定键名的数据/若键名不存在则返回false
     * @param string $key 键名
     * @param int $startIndex 开始下标，默认未0，即开头
     * @param int|null $length 获取长度，默认为null，即获取下标之后所有值
     * @return array|false
     */
    public function range($key, $startIndex = 0, $length = null)
    {
        if (!($keyHeader = $this->getKeyHeaderByKey($key, $handler))) {
            return false;
        }
        if (!$keyHeader['position']) {
            return [];
        }
        $values = [];
        $vHeaders = $this->getValueHeaders($handler, $keyHeader['position'], $startIndex, true, $length, true);
        foreach ($vHeaders as $key => $value) {
            $values[$key] = $value['value'];
        }
        return $values;
    }

    /**
     * 搜索指定键名是否包含某个值
     * @param string $key 键名
     * @param mixed $value 搜索值
     * @return int|bool 返回结果下标，未找到返回 false
     */
    public function search($key, $value)
    {
        if (!($keyHeader = $this->getKeyHeaderByKey($key, $handler)) || !$keyHeader['position']) {
            return false;
        }
        $headers = $this->getValueHeaders($handler, $keyHeader['position'], $value, false, 1);
        return $headers ? (int) key($headers) : false;
    }

    /**
     * 获取 key 的 header 数据
     * @param $key
     * @param null $handler
     * @param bool $lock
     * @return array|bool
     */
    protected function getKeyHeaderByKey($key, &$handler = null, $lock = false)
    {
        if (false === ($index = static::getKeyIndex($key)) || !($handler = $this->getHandler(self::HANDLER_GET))) {
            return false;
        }
        if ($lock && !flock($handler, LOCK_EX)) {
            return false;
        }
        // 若是 read 操作, 当前 handler 是不 lock 的, 那么写入优化缓存数据时, 要 lock 一下; 若是 write 操作, 就没必要了
        $keyHeader = $this->getKeyHeader($handler, $this->getKeyPosition($handler, $index), $key, false, !$lock, true);
        if ($lock && !$keyHeader) {
            flock($handler, LOCK_UN);
        }
        return $keyHeader;
    }

    /**
     * 读取指定 position 下 所有key 或 指定key 的 header 信息
     * [kLen, prev, next, position, now, key, current]
     * @param $handler
     * @param $position
     * @param null $key
     * @param bool $op  是否为优化过程来获取 header (直接查找,未找到情况下不尝试二次查找)
     * @param bool $lock 写入从优化过程提取数据时,是否需要加锁
     * @param bool $match 索引 header 是否必须与 key 匹配
     *                    $match=null: 尽可能尝试 match, 确实无法 match 返回 position 的header索引
     * @return array|bool
     */
    protected function getKeyHeader($handler, $position, $key = null, $op = false, $lock = false, $match = false)
    {
        $header = false;
        $position = (int) $position;
        while ($position > 0) {
            fseek($handler, $position, SEEK_SET);
            // keyLength/prevPosition/nextPosition/valuePosition - keyValue
            $header = unpack('vkLen/Vprev/Vnext/Vposition', fread($handler, 14));
            if (!is_array($header) || count($header) !== 4) {
                return $this->setError('Read key header failed');
            }
            $header['now'] = $position;
            $header['key'] = $header['kLen'] ? (string) fread($handler, $header['kLen']) : null;
            $header['current'] = false;
            if ($key === false) {
                return $header;
            }
            if ($key !== null) {
                if ($header['current'] = ($header['key'] === (string) $key)) {
                    return $header;
                }
            } elseif ($header['key']) {
                return $header;
            }
            $position = $header['next'];
        }
        $isMatch = $header && $header['current'];
        if (!$op && !$isMatch && ($match || $match === null) &&
            null !== ($resetHeader = $this->getKeyHeadersFromOptimize($handler, $key, $lock, (bool) $match))
        ) {
            // 从优化进程缓存文件 获取 key 数据成功并写入成功, 返回重新读取到的数据
            return $resetHeader;
        }
        return $match && !$isMatch ? false : $header;
    }

    /**
     * 如果刚好在优化过程中, 先从旧数据临时文件中提取数据并写入到当前数据文件
     * @param $handler
     * @param $key
     * @param bool $lock
     * @param bool $match
     * @return array|bool
     */
    protected function getKeyHeadersFromOptimize($handler, $key, $lock = false, $match = false)
    {
        if (false === ($index = static::getKeyIndex($key)) || !($opHandler = $this->getOptimizeHandler())) {
            return null;
        }
        // 从优化过程的缓存文件中 获取 key 的所有 values
        $keyHeader = $this->getKeyHeader($opHandler, $this->getKeyPosition($opHandler, $index), $key, true, false, true);
        $values = $keyHeader ?
            $this->getValueHeaders($opHandler, $keyHeader['position'], 0, true, null, true)
            : false;
        if (!$values) {
            return null;
        }
        $opValues = [];
        foreach ($values as $value) {
            $opValues[] = $value['value'];
        }
        if (!$this->writeKey($handler, $key, $index, $opValues, 'eof', true, true, $lock, true)) {
            return null;
        }
        return $this->getKeyHeader($handler, $this->getKeyPosition($handler, $index), $key, true, false, $match);
    }

    /**
     * 获取指定 key 的 lists header 数据 (lists header 中也可包含 value 数据)
     * @param $handler
     * @param int $position  key 的 lists 数据起始位置
     * @param bool $pivot    指定返回 lists 数据中 pivot 开始的数据
     *                       pivot=null, 则从开头获取
     *                       pivot>0, 从指定下标开始(包括pivot)
     *                       pivot<0, 从结尾向前数第 pivot 个值开始
     *                       pivot=string 从获取到对应 string 开始(包括 pivot)
     * @param bool $isIndex  说明 pivot 是否为下标 (或者是 值)
     * @param bool $length   >0 指定返回长度; length=null 则获取全部
     * @param bool $withValue 是否同时返回 value 数据 (否则仅返回链表 headers)
     * @return array
     */
    protected function getValueHeaders($handler, $position, $pivot = null, $isIndex = false, $length = null, $withValue = false)
    {
        $length = is_int($length) ? intval($length) : null;
        if ($length < 1) {
            $length = null;
        }
        if ($isIndex) {
            $pivot = is_int($pivot) ? (int) $pivot : null;
        } else {
            $pivot = strlen($pivot) ? static::crc(static::serialize((string) $pivot)) : null;
        }
        // 未指明 pivot 或 pivot<0 的情况下, 循环中的每个值都应被记录
        $find = $pivot === null || ($isIndex && $pivot < 0);
        $count = $len = 0;
        $headers = [];
        $diedLoop = [];

        while ($position > 0) {
            if (isset($diedLoop[$position])) {
                $this->setError('Read key list break by die loop');
                break;
            }
            $diedLoop[$position] = 1;
            $oneHeader = $this->getOneValueHeader($handler, $position, $withValue);
            if (!$oneHeader) {
                break;
            }
            // 判断是否从当前值开始后, 就应该被记录
            if (!$find) {
                $find = $isIndex ? $count >= $pivot : $oneHeader['crc'] === $pivot;
            }
            // 记录当前值
            if ($find) {
                $len++;
                $headers[$count] = $oneHeader;
                // 够数了 -> 跳出循环
                if ($length !== null && $len >= $length &&
                    (
                        !$isIndex ||
                        ($isIndex && ($pivot === null || $pivot >= 0)) // pivot<0 需全部获取
                    )
                ) {
                    break;
                }
            }
            $position = $oneHeader['next'];
            $count++;
        }
        // 需获取从结尾往前数 pivot 个的数据
        if ($isIndex && $pivot < 0) {
            $headers = array_slice($headers, $pivot, $length, true);
        }
        return $headers;
    }

    /**
     * 解包 list value 一个单元的数据
     * @param $handler
     * @param $position
     * @param bool $withValue
     * @return array|false
     */
    protected function getOneValueHeader($handler, $position, $withValue = false)
    {
        fseek($handler, $position, SEEK_SET);
        // valueLength/prevPosition/nextPosition/valueCrc - value
        $header = fread($handler, 12);
        $header = strlen($header) === 12 ? unpack('VvLen/Vprev/Vnext', $header) : null;
        if ($header && count($header) === 3 && $header['vLen'] && strlen($crc = fread($handler, 4)) === 4) {
            $header['crc'] = $crc;
        } else {
            return $this->setError('Read key list value failed');
        }
        $header['position'] = $position;
        if ($withValue) {
            $value = fread($handler, (int) $header['vLen']);
            $header['value'] = static::crc($value) === $header['crc'] ? static::unserialize($value) : null;
        }
        return $header;
    }

    /**
     * @param $handler
     * @param $seek
     * @param $int
     * @param bool $throw
     * @return $this|bool
     */
    protected function writePack($handler, $seek, $int, $throw = true)
    {
        fseek($handler, $seek, SEEK_SET);
        if (!$this->writeBufferToHandler($handler, pack('V', $int))) {
            $error = 'Write pack number ['.$int.'] failed on:'.$seek;
            if ($throw) {
                throw new RuntimeException($error);
            }
            return $this->setError($error);
        }
        return $this;
    }

    /**
     * @param mixed $offset
     * @return bool
     */
    public function offsetExists($offset)
    {
        return $this->exist($offset);
    }

    /**
     * @param mixed $offset
     * @return array|null
     */
    public function offsetGet($offset)
    {
        return $this->range($offset);
    }

    /**
     * @param mixed $offset
     * @param array $value
     */
    public function offsetSet($offset, $value)
    {
        $this->remove($offset);
        $this->setValue($value)->push($offset);
    }

    /**
     * @param mixed $offset
     */
    public function offsetUnset($offset)
    {
        $this->drop($offset);
    }

    /**
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
        $header = $this->getKeyHeader($handler, $position, false, true, false, false);
        if (!is_array($header)) {
            return 0;
        }
        $next = ($next = (int) $header['next']) === $position ? 0 : $next;
        $key = $header['key'];
        if ($onlyPosition || $key === null) {
            return $next;
        }
        $values = [];
        foreach ($this->getValueHeaders(
            $handler, $header['position'], null, true, null, true
        ) as $v) {
            $values[] = $v['value'];
        }
        return [
            'next' => $next,
            'key' => $key,
            'value' => $values
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
        $this->writeKey($handler, $key, $index, $value, 'eof', true, true, true, true);
    }
}
