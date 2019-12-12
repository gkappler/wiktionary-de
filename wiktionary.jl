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
prog = Progress(2*930126,"wikt:de entries") # *2 (start, done signals)



cache_size = 100
min_mem = 3*10^9
min_mem_juliadb = 10^9
state_channel=RemoteChannel(()->Channel(cache_size*10))
inbox=RemoteChannel(()->Channel(cache_size))
db_channel = RemoteChannel(()->Channel(100*cache_size))

wc=mc=0

@everywhere sleep_time = .001
@everywhere errorfile = joinpath(expanduser("~"),"ParserAlchemy.err.org")

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
    function process_entry(wt, inbox, db_channel, state_channel; log = false)
        make_org(s) = replace(s, r"^\*"m => " *")
        val = take!(inbox)
        starttime = time()
        put!(state_channel, ( myid(), :start, starttime, val.title ))
        if match(r"^(?:Modul):",val.title) !== nothing
            @info "Skipping $(val.title)"
        else
            try
                r,t = @timed tokenize(wt, val.revision.text; partial=:error);
                try
                    ntext = tokenize(wiktionary_defs,r; partial=:nothing)
                    if ntext !== nothing && match(r"^(?:Reim|Vorlage|Verzeichnis|Hilfe|Kategorie|Flexion):",val.title) === nothing
                        for v in ntext
                            for (w, ms) = wiki_meaning(v)
                                put!(db_channel, ("word", w))
                                for m in ms
                                    put!(db_channel, ("meaning", m))
                                end
                            end
                        end
                    else
                        put!(db_channel, ("page",(word=Token(Symbol("wikt:de"),val.title), page=r)))
                    end
                catch e
                    println("ERROR: save as page ", val.title)
                    open(errorfile, "a") do io
                        println(io, "* PAGE $(val.title)!")
                        println(io, "#+begin_src wikitext\n")
                        println(io, make_org(val.revision.text))
                        println(io, "\n#+end_src")
                        println(io, "** stacktrace")
                        println(io, "#+begin_src julia\n")
                        Base.showerror(io, e)
                        println(io, "\n#+end_src")
                    end
                    put!(db_channel, ("page", (word=Token(Symbol("wikt:de"),val.title), page=r)))
                end
            catch e
                println("ERROR: skipping ", val.title)
                open(errorfile, "a") do io
                    println(io, "* wikichunk error in $(val.title)")
                    if e isa ParserAlchemy.PartialMatchException 
                        println(io, "#+begin_src wikitext\n",make_org(context(e)),"\n#+end_src")
                        if e.str!=val.revision.text
                            println(io, "** subdata\n#+begin_src wikitext\n")
                            println(io, make_org(e.str))
                            println(io, "\n#+end_src")
                        end
                    end
                    println(io, "** data\n#+begin_src wikitext\n")
                    println(io, make_org(val.revision.text))
                    println(io, "\n#+end_src")
                    println(io, "** stacktrace")
                    println(io, "** subdata\n#+begin_src julia\n")
                    Base.showerror(io, e)
                    println(io, "\n#+end_src")
                end
                sleep(1)
            end
        end
        put!(state_channel, ( myid(), :done, time()-starttime, val.title ))
    end
    function process_wikitext(wt,inbox, db_channel,state_channel)
        @info "compiling" maxlog=1
        tokenize(wt,"a{{{b}}}[[c]]")
        @info "compiling done" maxlog=1
        while isopen(inbox) || isready(inbox)
            process_entry(wt,inbox, db_channel,state_channel)
            sleep(sleep_time)
        end
    end
    println("worker ready")
end

@info "start loading"

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
    # close(inbox)
    # while isready(inbox) || isready(db_channel)
    #     sleep(1)
    # end
    # close(db_channel)
end

## bind(inbox, read_task) ## close inbox when reading is done
open(errorfile,"w") do io
end

#parse_task = @async
xml_worker, page_workers = if workers()!=[1]
    workers()[1], workers()[2:end]
else
    1, workers()
end

@async xml_task = remote_do(process_xml, xml_worker, inbox, db_channel)
wt=wikitext(namespace = "wikt:de");

for p in page_workers## [1:end-1]
    remotecall(process_wikitext, p, wt, inbox, db_channel,state_channel)
end


function monitor(prog,state_channel; timeout=60)
    states=Dict{Int, Tuple{Symbol,Float64,String}}()
    while isopen(state_channel)
        while isready(state_channel)
            pid, status, t, title = take!(state_channel)
            states[pid] = (status, t, title)
            ProgressMeter.next!(prog; showvalues=[ ( (Symbol("pid$pid"), "$title - $status, $(trunc( ( status == :start ? t-time() : t )*1000)) ms") for (pid, (status, t, title)) in states)...,
                                                   (:mem_GB, Sys.free_memory()/10^9) ])
        end
        for (pid, (status, t, title)) in pairs(states)
            if status == :start && time()-t>timeout
                interrupt(pid)
                @warn "interrupting $title on pid $pid"
            end
        end
        sleep(1)
    end
end

mtask = @async monitor(prog, state_channel)

typevecs = TypePartitionChannel(db_channel,10000)

import Dates
datetimenow = Dates.format(Dates.now(),"Y-mm-dd_HHhMM")
output = expanduser("~/database/wiktionary-$datetimenow")
mkpath(output)
results = TypeDB(output)


dryrun = false
include("tablesetup.jl")


while isready(typevecs) || isopen(typevecs) || isopen(inbox)  || isopen(db_channel)

    global target,v_ = take!(typevecs);
    global v = token_lines.(v_);
    @info "indexing $(length(v)) $(target)"
    if Sys.free_memory() < min_mem_juliadb
        @info "saving data, free memory" (:mem_GB, Sys.free_memory()/10^9)
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

