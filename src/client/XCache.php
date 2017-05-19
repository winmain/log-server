<?
class XCache {
  /**
   * Методы semAcquire, semRelease для организации однопоточного доступа к определённому ресурсу.
   *
   * Пример использования:
   * <pre>
   * $semValue = semAcquire(SEM_TEST);
   * ... do some action ...
   * semRelease(SEM_TEST, $semValue);
   * </pre>
   */
  static public function semAcquire($semId, $ttlSeconds = 300) {
    $name = "semaphore-$semId";
    $value = microtime() . rand();
    while (true) {
      while (xcache_isset($name)) sleep(rand(0.01, 0.2));
      xcache_set($name, $value, $ttlSeconds);
      if (xcache_get($name) == $value) break;
    }
    return $value;
  }

  static public function semRelease($semId, $value) {
    $name = "semaphore-$semId";
    if (xcache_get($name) == $value) xcache_unset($name);
    else error_log("Invalid semaphore to unset for semId:$semId");
  }
}

define('SEM_SQL_LOG', 2);
