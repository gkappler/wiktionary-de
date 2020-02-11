
roots = NumID[ NumID{:word}(i) for i=1:size(X_.connections[:meaning])[1]]

models = Dict{Symbol,Tuple{LabeledArray,RelatedResponseModel}}()

GraphQLAlchemy.clear!()
@module_resolvers ParserAlchemy tokens
@module_resolvers TypeDBGraphs
@module_resolvers FilingForest
TableAlchemy.@graphql x [:meaning,:word,:TypedToken] [ Node ]
@module_resolvers OrgParser

"""
create a Trie with OutgoingEdge{Vector{reltype(G)},keytype(G)} keys.
Method keytype(G)(i::Int, a...) is assumed to create a key for the selections.
"""
function select_posterior(X::G, model, p, weights, a...) where G
    K = keytype(X|>typeof)
    R = reltype(X|>typeof)
    t = Trie{OutgoingEdge{Vector{R},K},Float64}()
    lpr,r = lpratio(p,weights,model.prior[1,:]; sparse=.1, scale=true, with_ranks=10)
    for (i,v) in zip(lpr.nzind, lpr.nzval)
        ps = paths(X, K(i,a...))
        for p in ps
            t[p...] = v
        end
    end
    t,r
    ## subtrie!(t,p...) # NumID.(vertex.(p))...)
end



search(x, s::String, a...; kw...) =
    search(x,filter(x -> variable(x)!=:delimiter,
                    tokenize(s)),
           a...; kw...)
search(x, v::Vector{Token}, a...; kw...) =
    search(x, value.(v), a...; kw...)


function search(x, v::Vector{String}, model::Symbol=:search, a...; kw...)
    predC,model = get!(models,model) do
        predC = x.connections[model]
        predC, RelatedResponseModel(predC.counts; alphaScale=1)
    end
    sdata = row(predC,v)
    P = FilingForest.predict(model, sdata, model.alphaP*eps())
    select_posterior(x, model, P, vcat(similar_to_weights, search_weights),  :meaning)
end


"""
TODO: use start
TODO: pagination
"""
function search(x, n::Integer, start::Vector{NumID}, model::Symbol,
           similar_to::Vector{Vector{NumID}},
           similar_to_weights::Vector{Float64},
           search::Vector{String},
           search_weights::Vector{Float64}
           )::TreeOrdering
    predC,model = get!(models,model) do
        predC = x.connections[model]
        predC, RelatedResponseModel(predC.counts; alphaScale=1)
    end
    sdata = ( (predC[s[end],:]
                   for (w,s) in zip(similar_to_weights,similar_to) )...,
                  (row(predC,value.(filter(x -> variable(x)!=:delimiter,
                                           tokenize(s))))
                   for (w,s) in zip(search_weights,search)
                   )... )
    P = FilingForest.predict(model, hcat(sdata...)', model.alphaP*eps())
    wP = vcat(similar_to_weights, search_weights)
    select_posterior(x, model, P, wP, :meaning)
end

search(X_,10,
       NumID[],
       :search,
       [NumID[id"meaning:1"],NumID[id"meaning:10"]],
       [1.0,-0.5],
       ["6 7"],
       [0.50])



@Query! function search(n::Integer, start::Vector{NumID}, typ::Symbol,
           similar_to::Vector{Vector{NumID}},
           similar_to_weights::Vector{Float64},
           search::Vector{String},
           search_weights::Vector{Float64}
           )::TreeOrdering
    # context.updateCounts!(context.termdata)
    Main.search(X_,n, start, typ,
           similar_to,
           similar_to_weights,
           search,
           search_weights)
end




# X_.connections[:meaning].row.database
# ## determining roots automatically might be a good idea, but first we do case-by-case
# roots=Dict{Symbol,AbstractArray}()
# for (rel,c) in X_.connections
#     @show rel
#     if rel in keys(X_.connections) && c.col isa TableAlchemy.Index
#         s = sum(c,dims=1)
#         if haskey(roots,c.col.database)
#             roots[c.col.database]=roots[c.col.database]+s
#         else
#             roots[c.col.database]=s
#         end
#     end
# end
# @show roots


import FilingForest: ls, descendants
"""
"""
function ls(pred::Function,recurse::Function,X_::TypeDBGraph,id::NumID, relations::Vector{Symbol},r=TreeValuesUpdate())
    s=r
    for (rel,c) in X_.connections
        if rel in relations && match(c.row,id)
            for i in c[id,:].nzind
                nid = NumID{c.col.database}(i)
                if pred(nid)
                    t = subtrie!(s,OutgoingEdge(
                        Symbol[rel],
                        nid
                    ))
                    recurse(nid) && ls(pred,recurse,X_,nid,relations,t)
                end
            end
        end
    end
    r
end

function ls(pred::Function,recurse::Function,X_::TypeDBGraph,path::Vector{<:NumID}, relations::Vector{Symbol},r=TreeValuesUpdate())
    if isempty(path)
        for p in roots
            s = subtrie!(r,OutgoingEdge(Symbol[],p))
            recurse(p) && ls(pred,recurse,X_,p,relations,s)
        end
        r
    else
        id = path[end]
        s=r
        for p in path
            s = subtrie!(s,OutgoingEdge(Symbol[],p))
            recurse(p) ## register p 
        end
        ls(pred,recurse,X_,path[end],relations,s)
        r
    end
end

@deprecate descendants(X_::TypeDBGraph,a...) ls(X_,a...;recurse=true)

function ls(X_::TypeDBGraph,a...;recurse=false)
    if recurse
        checked=Set{NumID}()        
        ls(f -> true,
           f-> if !(f in checked)
           push!(checked,f)
           true
           else
           false
           end,X_,a...)
    else
        ls(f->true,f->false,X_,a...)
    end
end


# A=ls(X_,[id"word:6"],[:meaning, :inflection, :meaning_token];recurse=true)
# A
# Symbol("")
# X_.connections



"List direct descendants that match glob. Roots if path is empty."
@Query! function ls(path::Vector{NumID}, relations::Vector{Symbol}, glob::String)::TreeValuesUpdate
    glob!="*" && error("todo: glob filtering")
    
    ls(X_,path,relations)
    ##context.tokencounts = update(context.tokencounts,context.data)
end

"Return all descendants at path.  Handle with care!"
@Query! function descendants(path::Vector{NumID}, relations::Vector{Symbol})::TreeValuesUpdate
    ls(X_,path,relations;recurse=true)
end
## @query descendants ["8dec96ea-67d2-46c8-8aa4-ff9b3285f790"]  "contains"

@Query! function test(paths::Vector{String})::Int
    1
end
println(resolver(:Query))


@Mutation! function save_body(id::ID, body::String, millis::Real)::Node
    created = Libc.strftime("%Y-%m-%d %a %H:%M", round(millis/1000))
    path = context.numeric_ids(context.trie, idpath)
    @show filepath = reverse(Main.map_path(identity, context.trie, path[end]))
    try 
        neu = Main.save_org!(context.trie,
                              filepath,
                              body;
                              CREATED=created)
        R=Main.select(Main.trie,
                      :Nothing, neu, path)
    catch e
        open("errors.org","a") do io
            write(io, "* savebody [[CREATED][$created]] error: $e\nidpath: $idpath\n",body)
        end
        @warn "$e error while saving, see errors.org" stacktrace(catch_backtrace())
    end
    ##context.tokencounts = update(context.tokencounts,context.data)
end
