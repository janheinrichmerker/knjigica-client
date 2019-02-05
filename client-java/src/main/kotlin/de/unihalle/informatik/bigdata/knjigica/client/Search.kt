package de.unihalle.informatik.bigdata.knjigica.client

import com.heinrichreimer.elasticsearch.kotlin.dsl.coroutines.rest.scrollAsync
import com.heinrichreimer.elasticsearch.kotlin.dsl.coroutines.rest.searchAsync
import com.heinrichreimer.elasticsearch.kotlin.dsl.rest.restHighLevelClientOf
import de.unihalle.informatik.bigdata.knjigica.client.extension.*
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import mbuhot.eskotlin.query.compound.dis_max
import mbuhot.eskotlin.query.fulltext.multi_match
import mbuhot.eskotlin.query.term.match_all
import mbuhot.eskotlin.query.term.term
import org.apache.http.HttpHost
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.common.unit.Fuzziness

object Search {

    private val HOST = HttpHost.create("http://localhost:9200")
    private val CLIENT = restHighLevelClientOf(HOST)

    private const val SCROLL_KEEP_ALIVE = "3m"
    private const val SCROLL_PAGE_SIZE = 50

    fun searchAsync(query: String): Deferred<SearchResponse> = GlobalScope.async { search(query) }

    fun searchAllAsync(query: String): Deferred<List<SearchResponse>> {
        return GlobalScope.async {
            val responses = mutableListOf<SearchResponse>()

            var response = search(query) {
                scroll(SCROLL_KEEP_ALIVE)
            }

            // Zero hits mark the end of the scroll and the while loop.
            while (response.scrollId != null && response.hits.hits.isNotEmpty()) {
                responses += response

                response = CLIENT.scrollAsync {
                    scrollId(response.scrollId)
                    scroll(SCROLL_KEEP_ALIVE)
                }
            }

            responses
        }
    }

    private suspend inline fun search(input: String, block: SearchRequest.() -> Unit = {}): SearchResponse {
        return CLIENT.searchAsync {
            indices = listOf(
                    IndexConfiguration.Annotation,
                    IndexConfiguration.Author,
                    IndexConfiguration.Opera,
                    IndexConfiguration.Plot,
                    IndexConfiguration.Role
            ).map(IndexConfiguration::index).toTypedArray()

            block()

            source {
                size = SCROLL_PAGE_SIZE

                query = dis_max {
                    queries = listOf(
                            // Annotations
                            bool {
                                must(
                                        term {
                                            "_type" to IndexConfiguration.Annotation.type
                                        },
                                        multi_match {
                                            type = "phrase"
                                            fields = listOf(
                                                    "operaTitle",
                                                    "title",
                                                    "text"
                                            )
                                            query = input
//                                fuzziness = Fuzziness.AUTO
                                            boost = 0.7f
                                        }
                                )
                            },
                            // Author
                            bool {
                                must(
                                        term {
                                            "_type" to IndexConfiguration.Author.type
                                        },
                                        multi_match {
                                            fields = listOf(
                                                    "operaTitle",
                                                    "name",
                                                    "fullName",
//                                                    "lifetime",
                                                    "scope"
                                            )
                                            query = input
                                            fuzziness = Fuzziness.AUTO
                                            boost = 1.5f
                                        }
                                )
                            },
                            // Opera
                            bool {
                                must(
                                        term {
                                            "_type" to IndexConfiguration.Opera.type
                                        },
                                        multi_match {
                                            fields = listOf(
//                                                    "premiere.date",
//                                                    "premiere.place",
                                                    "title",
                                                    "subtitle",
                                                    "language"
                                            )
                                            query = input
                                            fuzziness = Fuzziness.AUTO
                                            boost = 3f
                                        }
                                )
                            },
                            // Plot
                            bool {
                                must(
                                        term {
                                            "_type" to IndexConfiguration.Plot.type
                                        },
                                        multi_match {
                                            type = "phrase"
                                            fields = listOf(
                                                    "operaTitle",
                                                    "section.ACT",
                                                    "section.SCENE",
                                                    "section.NUMBER",
                                                    "roleName",
                                                    "text",
                                                    "instruction"
                                            )
                                            query = input
//                                fuzziness = Fuzziness.AUTO
                                            boost = 0.15f
                                        }
                                )
                            },
                            // Roles
                            bool {
                                must(
                                        term {
                                            "_type" to IndexConfiguration.Role.type
                                        },
                                        multi_match {
                                            fields = listOf(
                                                    "operaTitle",
                                                    "name",
                                                    "description",
                                                    "voice",
                                                    "note"
                                            )
                                            query = input
                                            fuzziness = Fuzziness.AUTO
                                            boost = 1.2f
                                        }
                                )
                            }
                    )
                }
            }
        }
    }
}
