@file:DependsOn("org.jetbrains.kotlinx:dataframe:0.10.0-dev-1532")

import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.api.column
import org.jetbrains.kotlinx.dataframe.api.convert
import org.jetbrains.kotlinx.dataframe.api.dropLast
import org.jetbrains.kotlinx.dataframe.api.groupBy
import org.jetbrains.kotlinx.dataframe.api.into
import org.jetbrains.kotlinx.dataframe.api.rename
import org.jetbrains.kotlinx.dataframe.api.reorderColumnsByName
import org.jetbrains.kotlinx.dataframe.api.sumOf
import org.jetbrains.kotlinx.dataframe.io.read
import org.jetbrains.kotlinx.dataframe.io.writeCSV

val DurationRaw by column<String>("Duration")
val Comment by column<String>()

val data = DataFrame.read(args[0])[DurationRaw, Comment]
    .dropLast(4)
    .groupBy(Comment)
    .sumOf { it[DurationRaw].parseHHMM().inWholeSeconds }
    .convert("sum") { (it as Long).seconds }
    .rename("sum").into("Duration")
    .reorderColumnsByName(desc = true)
println(data)
data.writeCSV("output.csv")

fun String.parseHHMM(): Duration {
    val (hh, mm) = split(':').map(String::toInt)
    return hh.hours + mm.minutes
}
