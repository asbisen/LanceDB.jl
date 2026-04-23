# Unit test for Bug #3: _make_reader leaks arr_ptr/schema_ptr on error.
#
# Code path (arrow_data.jl):
#   function _make_reader(data)
#       arr_ptr, schema_ptr, pins = _to_arrow_c_abi(data)   ← allocated here
#       reader_out = ...
#       errmsg     = ...
#       GC.@preserve pins begin
#           code = lancedb_record_batch_reader_from_arrow(
#               Ptr{Cvoid}(arr_ptr), Ptr{Cvoid}(schema_ptr), reader_out, errmsg)
#       end
#       check(code, errmsg)   ← throws here on error — arr_ptr/schema_ptr leak
#       reader_out[], schema_ptr, arr_ptr, pins
#   end
#
# Trigger: corrupt a child schema's format string to something Arrow-rs's
# Schema::try_from does not recognise ("ZZZZ_UNKNOWN_FORMAT"). The C call
# returns LANCEDB_ARROW, so check() would throw and the two heap allocations
# produced by _to_arrow_c_abi are never freed.
#
# This test replicates the internals of _make_reader step-by-step and
# manually frees arr_ptr/schema_ptr at the end to avoid a real leak in the
# test suite itself.

@testset "_make_reader leaks arr_ptr/schema_ptr on error (bug #3)" begin

    data = (x = Int32[1, 2, 3],)

    # Step 1: allocate exactly what _make_reader allocates.
    arr_ptr, schema_ptr, pins = LanceDB._to_arrow_c_abi(data)

    # Step 2: corrupt the first child schema's format string to a value that
    # Arrow-rs's Schema::try_from will not recognise, causing the C call to
    # fail with LANCEDB_ARROW.
    #
    # Memory layout after _to_arrow_c_abi(data=(x=Int32[...],)):
    #   schema_ptr  →  ArrowSchema{ format="+s", n_children=1,
    #                      children → [ child_ptr → ArrowSchema{ format="i", name="x" } ] }
    root      = unsafe_load(schema_ptr)
    ch_ptrs   = Ptr{Ptr{LanceDB.ArrowSchema}}(root.children)
    child_ptr = unsafe_load(ch_ptrs, 1)    # pointer to the "x: Int32" schema
    child     = unsafe_load(child_ptr)
    Base.Libc.free(child.format)           # free the original "i" string
    bad_fmt   = LanceDB._malloc_cstr("ZZZZ_UNKNOWN_FORMAT")
    unsafe_store!(child_ptr, LanceDB.ArrowSchema(
        bad_fmt,          child.name,       child.metadata,
        child.flags,      child.n_children, child.children,
        child.dictionary, child.release,    child.private_data,
    ))

    # Step 3: mirror the C call from _make_reader.
    reader_out = Ref{Ptr{LanceDB.LanceDBRecordBatchReaderHandle}}(C_NULL)
    errmsg     = Ref{Ptr{UInt8}}(C_NULL)
    local code
    GC.@preserve pins begin
        code = LanceDB.lancedb_record_batch_reader_from_arrow(
            Ptr{Cvoid}(arr_ptr), Ptr{Cvoid}(schema_ptr), reader_out, errmsg)
    end

    # The corrupt format string must cause a failure — prove the error path exists.
    @test code != Cint(LanceDB.LANCEDB_SUCCESS)

    # Drain the error string exactly as check() would.
    errmsg[] != C_NULL && LanceDB.lancedb_free_string(errmsg[])

    # Bug: _make_reader calls check(code, errmsg) at this point, which throws.
    # arr_ptr and schema_ptr are then never freed — they leak.
    # We free them manually here to avoid an actual leak in the test suite.
    LanceDB._free_array_tree(arr_ptr)
    LanceDB.release_arrow_schema(schema_ptr)
end
