package de.unihalle.informatik.bigdata.knjigica.client

import de.unihalle.informatik.bigdata.knjigica.client.extension.isEmpty
import de.unihalle.informatik.bigdata.knjigica.client.extension.totalHits
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.runBlocking
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.SearchHit

@ObsoleteCoroutinesApi
@ExperimentalCoroutinesApi
object SearchCli {

    private val DEFAULT_TYPE = Search.QueryType.PLOT

    @JvmStatic
    fun main(args: Array<String>) {
        runBlocking {
            runSearch(args.getOrNull(0), args.getOrNull(1))
        }
        System.exit(0)
    }

    private suspend fun runSearch(typeArg: String?, queryArg: String?) {
        val type = getType(typeArg)
        val query = getQuery(type, queryArg)

        println("Searching $type for query '$query'.")
        val channel: ReceiveChannel<SearchResponse> = Search.searchAll(type, query)
        printResults(channel)
        println()
        println("Good bye!")
    }

    private fun getType(firstGuess: String?): Search.QueryType {

        fun guessType(guess: String?): Search.QueryType? {
            return when (guess) {
                "plot" -> Search.QueryType.PLOT
                else -> null
            }
        }

        var type: Search.QueryType? = guessType(firstGuess)
        if (type == null) {
            print("Would you like to search the $DEFAULT_TYPE index ? (y/N) ")
            if (readLine()?.toLowerCase() == "y") {
                type = DEFAULT_TYPE
            }
        }
        while (type == null) {
            print("Specify query type: ")
            type = guessType(readLine())
        }
        return type
    }

    private fun getQuery(type: Search.QueryType, first: String?): String {
        var query: String? = first
        while (query == null) {
            print("Type in query to search $type for: ")
            query = readLine()
        }
        return query
    }

    private fun ReceiveChannel<SearchResponse>.printGlobalStats(): ReceiveChannel<SearchResponse> {
        return mapIndexed { index, response ->
            if (index == 0) {
                if (response.isEmpty()) {
                    println("Sorry! Found no hits.")
                } else {
                    println("Found ${response.totalHits} hits in ${response.took}:")
                }
                println()
            }

            response
        }
    }

    private fun ReceiveChannel<SearchResponse>.flatMapHits(): ReceiveChannel<SearchHit> {
        return map { it.hits }
                .flatMap { hits ->
                    GlobalScope.produce {
                        hits.hits.forEach {
                            send(it)
                        }
                    }
                }
    }

    private fun <T : Any> ReceiveChannel<T>.disturb(counter: Int = 1): ReceiveChannel<T> {
        return mapIndexed { index, element -> index to element }
                .takeWhile { (index, _) ->
                    if (index % counter == 0 && index != 0) {
                        println("Would you like to see more? (Y/n)")
                        val shouldCancel = readLine()?.toLowerCase() == "n"
                        !shouldCancel
                    } else true
                }
                .map { (_, element) -> element }
    }

    private suspend fun printResults(channel: ReceiveChannel<SearchResponse>) {
        channel.printGlobalStats()
                .disturb()
                .flatMapHits()
                .printHits()
    }

    private suspend fun ReceiveChannel<SearchHit>.printHits() = consumeEach(::printHit)

    private fun printHit(hit: SearchHit) {
        val source: Map<String, Any> = hit.sourceAsMap
        val roleName = source["roleName"]?.toString()
        @Suppress("unchecked_cast")
        val section = source["section"] as Map<String, Any>
        val act = section["ACT"]?.toString()
        val scene = section["SCENE"]?.toString()
        val text = source["text"]?.toString()
        val instruction = source["instruction"]?.toString()
        println("${roleName ?: "All"} at $act, $scene (hit score = ${hit.score}):")
        if (instruction != null) {
            println(instruction.prependIndent("! "))
        }
        if (text != null) {
            println(text.prependIndent("> "))
        }
        println()
    }
}