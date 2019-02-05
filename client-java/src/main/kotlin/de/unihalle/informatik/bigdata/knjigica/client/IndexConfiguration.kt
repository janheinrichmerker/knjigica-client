package de.unihalle.informatik.bigdata.knjigica.client

sealed class IndexConfiguration(
        val index: String,
        val type: String
) {
    object Libretto : IndexConfiguration("libretti", "libretto")
    object Annotation : IndexConfiguration("annotations", "annotation")
    object Author : IndexConfiguration("authors", "author")
    object Opera : IndexConfiguration("operas", "opera")
    object Plot : IndexConfiguration("plots", "plot")
    object Role : IndexConfiguration("roles", "role")

    companion object {
        val ALL = arrayOf(
                Libretto,
                Annotation,
                Author,
                Opera,
                Plot,
                Role
        )
    }
}