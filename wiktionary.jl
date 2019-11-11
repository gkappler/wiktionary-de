# using Parameters
# Wiktionary = @with_kw (title="", id=ID(""), parentid=ID(""), timestamp="", contributor=(user="anonym", id=ID("")), comment="",
#                        model="", format="", text="", sha1="", ns="", revision="")
##NamedTuple{(:title, :id, :ns, :revision, :revision), Tuple{String, String, String, String}}
##convert(Wiktionary, (;[ "ns" => "a", "revision" => "b" ]...))
cd(expanduser("~/dev/julia/"))
using Pkg
Pkg.activate("wiktionary")
using Distributed

#addprocs(1)
@info "using $(nprocs()) prcs $(nworkers()) workers"
# using Revise
@everywhere  using Pkg
@everywhere    Pkg.activate("wiktionary")
@everywhere    Pkg.resolve()
@everywhere    Pkg.instantiate()
@everywhere begin
    println("loading TableAlchemy")
    cd(expanduser("~/dev/julia/"))
    using TableAlchemy
    using ParserAlchemy
    using ParserAlchemy.Tokens
    println("worker ready")
end


# starting a remote-process is ease, yeay!
# mtree = addprocs(["mytree@mytree-analytics.de:2222"];
#                  tunnel = true,
#                  dir = "/home/mytree/deploy",
#                  exename = "/home/mytree/julia-1.1.0/bin/julia")


using ProgressMeter
prog = ProgressUnknown("Wiktionary indexing:")
dprog = ProgressUnknown("data:")

cache_size = 100
min_mem = 3*10^9
min_mem_juliadb = 10^9
@everywhere sleep_time = .001
inbox=RemoteChannel(()->Channel(cache_size))
db_channel = RemoteChannel(()->Channel(cache_size*10))

@everywhere errorfile = joinpath(expanduser("~"),"ParserAlchemy.err")
open(errorfile,"w") do io
end
@everywhere begin
    println("loading FilingForest")
    cd(expanduser("~/dev/julia/"))
    using FilingForest
    using ParserAlchemy
    using OrgParser
    using ParserAlchemy.Tokens
    using WikitextParser
    import ProgressMeter
    import ProgressMeter: next!
    import WikitextParser: wikitext
    function wikichunks(inbox, output;
                        wikitextParser = wikitext(namespace = "wikt:de"),
                        prog=nothing, wait_onwarn = false, log = false, errorfile=nothing)
        val = take!(inbox);
        prog !== nothing && ProgressMeter.next!(prog; showvalues=[(:parsing, val.title)])
        ntext = try
            r=tokenize(wikitextParser, val.revision.text; errorfile=errorfile)
            r === nothing ? nothing : tokenize(wiktionary_defs,r, delta=3)
        catch e
            if wait_onwarn ##&& i < lastindex(val.revision.text)
                print("inspect and press <ENTER>")
                readline()
            end
            rethrow(e)
        end        
        if ntext !== nothing
            for v in ntext
                for (w, ms) = wiki_meaning(v)
                    put!(output,("word", w))
                    for m in ms
                        put!(output, ("meaning", m))
                    end
                end
            end
        end
    end
    function process_wikitext(inbox, db_channel)
        while isopen(inbox) || isready(inbox)
            try
                @info "compiling" maxlog=1
                wikichunks(inbox, db_channel; errorfile=errorfile)
                @info "compiling done" maxlog=1
            catch e
                sleep(1)
                @warn "cannot parse meanings in wiktionary" exception=e
                ## rethrow(e)
            end
            sleep(sleep_time)
        end
    end
    println("worker ready")
end

@info "start loading"

wc=mc=0
@everywhere function process_xml(inbox, db_channel; progress=nothing)
    try
        parse_bz2(expanduser("~/data/dewiktionary-latest-pages-articles.xml.bz2")) do val, counter
            progress !== nothing && ProgressMeter.next!(
                progress; showvalues=[(:word, val.title), (:mem, Sys.free_memory()/10^6) ])
            put!(inbox, val)
            sleep(sleep_time)
        end
    catch e
        @error "xml parsing" exception=e
    end
    close(inbox)
    while isready(inbox) || isready(db_channel)
        sleep(1)
    end
    close(db_channel)
end
## bind(inbox, read_task) ## close inbox when reading is done

#parse_task = @async
xml_worker, page_workers = if workers()!=[1]
    workers()[1], workers()[2:end]
else
    1, workers()
end

@async xml_task = remote_do(process_xml, xml_worker, inbox, db_channel)


for p in page_workers## [1:end-1]
    remotecall(process_wikitext, p, inbox, db_channel)
end



typevecs=TypePartitionChannel(db_channel,10000)
show_wiki(x) = let w=x.word, m=haskey(x,:meaning) ? ": $(x.meaning)" : ""
    r = "$w$m"
    if lastindex(r)>60        
        r[1:nextind(r,60)]
    else
        r
    end
    ProgressMeter.next!(dprog; showvalues=[(:entry, r), (:mem_GB, Sys.free_memory()/10^9) ])
end


TableAlchemy._db_name(::Type{<:NamedString}) = "Indent"

import Dates
datetimenow = Dates.format(Dates.now(),"Y-mm-dd_HHhMM")
output = expanduser("~/database/wiktionary-$datetimenow")
mkpath(output)
results = TypeDB(output)

begin ## setup
    using ParserAlchemy.Tokens
    
    clearnames!(results)
    db_name!(results, Token, :Token)
    db_intern!(results, Token)
    db_name!(results, LinePrefix{NamedString}, :Prefix)
    db_intern!(results, LinePrefix{NamedString})
    db_name!(results, Line{NamedString,Token}, :Line)
    db_name!(results, Node{Line{Token,AbstractToken}}, :Node)
    db_name!(results, WikiLink, :WikiLink)
    db_name!(results, String, :String)
    db_name!(results, Symbol, :Symbol)
    db_name!(results, TokenPair{Symbol,Vector{LineContent}}, :TokenPair)
    db_name!(results, Template{Token,LineContent}, :Template)
    db_name!(results, Pair{String,Paragraph}, :TemplateArgument)
    db_name!(results, Paragraph{Token,LineContent}, :Paragraph)
    db_name!(results, Pair{String,Vector{Line{Token,LineContent}}},
             Symbol("Pair{String,Paragraph}"))
    vector_index!(results, Vector{Token}, :token)
    vector_index!(results, Vector{LineContent}, :token)
    vector_index!(results, Paragraph{NamedString,Token}, :line)
    results.type_names
end

dryrun = false

import ParserAlchemy.Tokens: token_lines
token_lines(x::NamedTuple{ns,ts}) where {ns,ts} =
    (; ( ( n => (t <: Vector{<:Line} ? token_lines(getproperty(x,n); typenames=results.type_names) : getproperty(x,n)))
         for (n,t) in zip(ns, fieldtypes(ts)) )...
     )
token_lines(x::NamedStruct{name}) where {name} =
    NamedStruct{name}(token_lines(get(x)))

while isready(typevecs) || isopen(typevecs) || isopen(inbox)  || isopen(db_channel)
    global target,v_ = take!(typevecs);
    global v = token_lines.(v_);
    println()
    @info "indexing $(length(v)) $(eltype(v))"
    println()
    if Sys.free_memory() < min_mem_juliadb
        @info "memory pressure: saving data" (:mem_GB, Sys.free_memory()/10^9)
        TableAlchemy.save(results)
        @info "saved data" (:mem_GB, Sys.free_memory()/10^9)
    end

    db_name!(results, eltype(v), target)
    # try
    pkeys_tuple!(results, eltype(v), :word, :numid)
    if !dryrun
        db, ks, jk = TableAlchemy.push_pkeys!(
            results,
            emptykeys(v), TypedIterator(v));
    end
    v = nothing
    Sys.GC.gc()

    # catch e
    #     @error "cannot push into db" exception=e
    # end
end

TableAlchemy.save(results)

