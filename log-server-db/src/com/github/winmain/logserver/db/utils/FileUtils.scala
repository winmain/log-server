package com.github.winmain.logserver.db.utils

import java.nio.file.{Files, Path, Paths}
import java.util.zip.GZIPOutputStream

object FileUtils {
  /**
   * Обрезать окончание имени файла, если оно совпадает с заданной строкой #ending.
   * Если файл обрезан, то возращаем Some(newPath), если же нет такого окончания, то None.
   */
  def maybeChopEnding(path: Path, ending: String): Option[Path] = {
    val name: String = path.getFileName.toString
    if (name.endsWith(ending)) {
      Some(path.resolveSibling(name.substring(0, name.length - ending.length)))
    } else None
  }

  /**
   * Сжать файл #path и вернуть новое имя сжатого файла.
   */
  def gzipFile(path: Path): Path = {
    val gzPath = Paths.get(path.toString + ".gz")
    val output: GZIPOutputStream = new GZIPOutputStream(Files.newOutputStream(gzPath), 8192)
    Files.copy(path, output)
    output.close()
    gzPath
  }
}
