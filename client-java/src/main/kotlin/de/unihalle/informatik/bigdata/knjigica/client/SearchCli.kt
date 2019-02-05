package de.unihalle.informatik.bigdata.knjigica.client

import de.unihalle.informatik.bigdata.knjigica.client.extension.isEmpty
import de.unihalle.informatik.bigdata.knjigica.client.extension.totalHits
import kotlinx.coroutines.runBlocking
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.SearchHit

object SearchCli {

    @JvmStatic
    fun main(args: Array<String>) {
        runBlocking {
            runSearch(args.getOrNull(0))
        }
        System.exit(0)
    }

    private suspend fun runSearch(queryArg: String?) {
        val query = getQuery(queryArg)

        println("Searching for '$query'.")
        val searchResults = Search.searchAllAsync(query).await()
        searchResults.printResults()
        println()
        println("Good bye!")
    }

    private fun getQuery(first: String?): String {
        var query: String? = first
        while (query == null) {
            print("Type in query: ")
            query = readLine()
        }
        return query
    }

    private fun List<SearchResponse>.printGlobalStats() {

        val first = firstOrNull()
        if (first?.isEmpty() != false) {
            println("Sorry! Found no hits.")
        } else {
            println("Found ${first.totalHits} hits in ${first.took}:")
        }
        println()
    }

    private fun List<SearchResponse>.flatMapHits(): List<SearchHit> = flatMap { it.hits.hits.asList() }

    private fun <T : Any> List<T>.disturb(counter: Int = 1): Sequence<T> {
        var first = true
        return chunked(counter)
                .asSequence()
                .takeWhile {
                    if (first) {
                        first = false
                        true
                    }
                    else {
                        println("Would you like to see more? (Y/n)")
                        val shouldCancel = readLine()?.toLowerCase() == "n"
                        !shouldCancel
                    }
                }
                .flatMap { it.asSequence() }
    }

    private fun List<SearchResponse>.printResults() {
        printGlobalStats()
        flatMapHits()
                .disturb(counter = 8)
                .printHits()
    }

    private fun Sequence<SearchHit>.printHits() = forEach { it.printHit() }

    private fun SearchHit.printHit() {
        when (type) {
            IndexConfiguration.Plot.type -> printPlotHit()
            IndexConfiguration.Role.type -> printRoleHit()
            IndexConfiguration.Annotation.type -> printAnnotationHit()
            IndexConfiguration.Author.type -> printAuthorHit()
            IndexConfiguration.Opera.type -> printOperaHit()
            else -> println(this)
        }
        println()
    }

    private fun SearchHit.printPlotHit() {
        println("Plot (hit score = $score):")
        val source: Map<String, Any> = sourceAsMap
        val operaTitle = source["operaTitle"]?.toString()
        val roleName = source["roleName"]?.toString()?.toUpperCase() ?: "ALL"
        @Suppress("unchecked_cast")
        val section = source["section"] as Map<String, Any>
        val act = section["ACT"]?.toString()
        val scene = section["SCENE"]?.toString()
        val text = source["text"]?.toString()
        val instruction = source["instruction"]?.toString()
        println("$roleName in \"$operaTitle\" at '$act', '$scene':")
        if (instruction != null) {
            println(instruction.prependIndent("=> "))
        }
        if (text != null) {
            println(text.prependIndent(">> "))
        }
    }

    private fun SearchHit.printRoleHit() {
        println("Role (hit score = $score):")
        val source: Map<String, Any> = sourceAsMap
        val operaTitle = source["operaTitle"]?.toString()
        val name = source["name"]?.toString()?.toUpperCase()
        val description = source["description"]?.toString()
        val voice = source["voice"]?.toString()?.toLowerCase()?.replace('_', ' ')
        println("$name${description?.let { " ($it)" } ?: ""}${voice?.let {" - $voice"} ?: ""} in \"$operaTitle\"")
    }

    private fun SearchHit.printOperaHit() {
        println("Opera (hit score = $score):")
        val source: Map<String, Any> = sourceAsMap
        val title = source["title"]?.toString()?.toUpperCase()
        val subtitle = source["subtitle"]?.toString()
        val language = source["language"]?.toString()
        println("\"$title\"${subtitle?.let { " - \"$it\"" } ?: ""}${language?.let {" ($it)"} ?: ""}")
    }

    private fun SearchHit.printAuthorHit() {
        println("Author (hit score = $score):")
        val source: Map<String, Any> = sourceAsMap
        val name = source["name"]?.toString()?.toUpperCase()
        val fullName = source["fullName"]?.toString()?.takeIf { it != name }
        val scope = source["scope"]?.toString()?.toLowerCase()
        val operaTitle = source["operaTitle"]?.toString()
        println("$name${fullName?.let { " ($it)" } ?: ""}")
        if (operaTitle != null && scope != null) {
            println("~> $scope for \"$operaTitle\"")
        }
    }

    private fun SearchHit.printAnnotationHit() {
        println("Annotation (hit score = $score):")
        val source: Map<String, Any> = sourceAsMap
        val operaTitle = source["operaTitle"]?.toString()
        val title = source["title"]?.toString()?.toUpperCase()
        val text = source["text"]?.toString()
        println("$title about \"$operaTitle\"")
        if (text != null) {
            println(text.prependIndent("~> "))
        }
    }
}