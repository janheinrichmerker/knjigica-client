package de.unihalle.informatik.bigdata.knjigica.client

import com.heinrichreimer.elasticsearch.kotlin.dsl.coroutines.rest.scrollAsync
import com.heinrichreimer.elasticsearch.kotlin.dsl.coroutines.rest.searchAsync
import com.heinrichreimer.elasticsearch.kotlin.dsl.rest.restHighLevelClientOf
import de.unihalle.informatik.bigdata.knjigica.client.extension.*
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import org.apache.http.HttpHost
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.search.sort.FieldSortBuilder



object Search {

    private val HOST = HttpHost.create("http://localhost:9200")
    private val CLIENT = restHighLevelClientOf(HOST)

    private const val SCROLL_KEEP_ALIVE = "3m"
    private const val SCROLL_PAGE_SIZE = 5

    private const val PLOT_INDEX = "plots"
    private const val PLOT_TYPE = "plot"

    enum class QueryType {
        PLOT
    }

    fun search(type: QueryType, query: String): Deferred<SearchResponse> =
            GlobalScope.async { searchAsync(type, query) }

    @ExperimentalCoroutinesApi
    fun searchAll(type: QueryType, query: String): ReceiveChannel<SearchResponse> {
        return GlobalScope.produce {
            var response = searchAsync(type, query) {
                scroll(SCROLL_KEEP_ALIVE)
            }

            do {
                send(response)

                response = CLIENT.scrollAsync {
                    scrollId(response.scrollId)
                    scroll(SCROLL_KEEP_ALIVE)
                }
            } while (response.hits.hits.isNotEmpty()) // Zero hits mark the end of the scroll and the while loop.
        }
    }

    private suspend inline fun searchAsync(type: QueryType, query: String, block: SearchRequest.() -> Unit = {}): SearchResponse {
        return when (type) {
            QueryType.PLOT -> searchPlot(query, block)
        }
    }

    private suspend inline fun searchPlot(input: String, block: SearchRequest.() -> Unit = {}): SearchResponse {
        return CLIENT.searchAsync {
            indices = arrayOf(PLOT_INDEX)

            block()

            source {
                size = SCROLL_PAGE_SIZE

                queryBool {
                    should(
                            "roleName" match input,
                            "text" match input
                    )
                }
            }
        }
    }
}