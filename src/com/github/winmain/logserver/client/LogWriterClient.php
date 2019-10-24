<?

class LogWriterClient {

  private $writeDir;
  private $savedFileFormat;
  private $fileLifetimeMillis;

  const version = 1;
  const charset = 'utf-8';

  public function __construct($writeDir, $savedFileFormat = 'Ymd\THis\-\p\h\p\.\s\a\v\e\d', $fileLifetimeSeconds = 300) {
    $this->writeDir = $writeDir;
    $this->savedFileFormat = $savedFileFormat;
    $this->fileLifetimeMillis = $fileLifetimeSeconds;
  }

  public function append($normalizedTableName, $maybeId, $log) {
    if (!is_null($maybeId) && !is_int($maybeId)) throw new Exception("Invalid type of maybeId:$maybeId");
    if (!is_string($normalizedTableName)) throw new Exception("Invalid type of normalizedTableName:$normalizedTableName");
    if (!is_string($log)) throw new Exception("Invalid type of log:$log");

    $sem = XCache::semAcquire(SEM_SQL_LOG);

    $time = time();
    // file autorotate
    $fname = $this->currentFileName();
    if (file_exists($fname)) {
      $tname = $this->timeFileName();
      if (!file_exists($tname) || $time >= intval(file_get_contents($tname)) + $this->fileLifetimeMillis) {
        rename($fname, $this->writeDir . date($this->savedFileFormat));
        file_put_contents($tname, $time);
      }
    } else {
      $dirname = dirname($fname);
      if (!file_exists($dirname)) {
        mkdir($dirname, 0777, true);
      }
      file_put_contents($this->timeFileName(), $time);
    }

    if (!file_exists($fname)) {
      file_put_contents($fname, pack('N', self::version));  // write file version
    }

    $fp = fopen($fname, "a");
    // acquire an exclusive lock
    while (!flock($fp, LOCK_EX))
      sleep(rand(0, 100) / 1000.0);

    // write data
    $this->writeStr($fp, $normalizedTableName);
    if (is_null($maybeId)) {
      fwrite($fp, pack('C', 0));
    } else {
      fwrite($fp, pack('CN', 1, $maybeId));
    }
    $this->writeStr($fp, $log);

    fflush($fp);
    flock($fp, LOCK_UN);
    fclose($fp);

    XCache::semRelease(SEM_SQL_LOG, $sem);
  }

  private function currentFileName() {
    return $this->writeDir . '/current-php';
  }
  private function timeFileName() {
    return $this->writeDir . '/time-php';
  }

  private function writeStr($fp, $str) {
    $s = iconv('cp1251', self::charset, $str);
    fwrite($fp, pack('N', strlen($s)));
    fwrite($fp, $s);
  }
}