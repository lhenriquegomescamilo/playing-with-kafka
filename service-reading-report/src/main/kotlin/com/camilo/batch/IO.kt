package com.camilo.batch

import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption

class IO {
    companion object {
        fun copyTo(source: Path, target: File) {
            target.parentFile.mkdirs();
            Files.copy(source, target.toPath(), StandardCopyOption.REPLACE_EXISTING)
        }

        fun append(target: File, content: String) {
            Files.write(target.toPath(), content.toByteArray(), StandardOpenOption.APPEND)
        }
    }
}
