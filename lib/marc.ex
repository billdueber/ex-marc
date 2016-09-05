defmodule Marc do
  @moduledoc """
  # Abstract MARC21 library for Elixir

  Bare-bones MARC routines for dealing with library MARC
  records in the most common format I deal with
  (single-char indicators, three-character tags, single-char
  subfield codes, all UTF-8).
  """


  defmodule Record do
    @moduledoc """
    An abstract MARC record, consisting strictly of a leader
    and a list of fields
    """
    defstruct leader: nil, fields: []

    def find(r, tag) when is_binary(tag) do
      Enum.find(r.fields, fn(f) -> f.tag == tag end)
    end

    def field(r,tag) when is_binary(tag), do: find(r,tag)

    @doc """
    Find the first field with the given tag

    Will remind myself not to be an idiot and send tags
    in single-quotes or send a list of tags.
    Should remove that at some point.
    """
    def find(r, tag) when is_list(tag) do
      IO.puts "Use a string for the tag, dummy"
      nil
    end

    @doc """
    Return the list of fields for the record.

    * When called wtih no arguments, returns all fields
    * When given a list of tags, only return fields that have
      one of those tags
    """

    def fields(r) do
      r.fields
    end

    defp matches_at_least_one(field, tags) do
      Enum.any? tags, fn(x) -> field.tag == x end
    end

    def fields(r, tags) when is_list(tags) do
      r.fields
        |> Enum.filter(fn(f) -> matches_at_least_one(f, tags) end)
    end

    def fields(r, tag) when is_binary(tag) do
      fields(r, [tag])
    end

    def values(record, tag) when is_binary(tag) do
      record
        |> Record.fields(tag)
        |> Enum.map(&Marc.Field.value/1)
    end

    def value(record, tag) when is_binary(tag) do
      record
        |> values(tag)
        |> List.first || ""
    end

  end

  defprotocol Field do
    @doc """
    Return the value(s) of the field as a single string.
    For Control fields, just return the value. For DataFields,
    return a join of all the values of all the subfields (in order).
    The default join character is a single space, but you can pass
    one if you'd like.

    ## Examples

    Get the title
        record |> Marc.Record.find("245") |> Marc.Field.value

    Get a list of all values, with pipes between multiple subfield values

        record |> Marc.Record.fields |> Enum.map(&Marc.Field.value(&1, "|"))
    """
    def tag(field)
    def value(field)
    def value(field, joiner)

    @doc "Report the field type"
    def type(field)
  end


  defmodule ControlField do
    @moduledoc """
    A ControlField -- just a tag and a value
    """
    defstruct tag: nil, value: nil

    defimpl Field, for: ControlField do
      @doc """
      Value of the field.
      """

      def value(f, joiner \\ nil) do
        f.value
      end

      @doc """
      Get the tag of the controlfield
      """
      def tag(f), do: f.tag

      @doc "Type is always :control"
      def type(f), do: :control
    end


  end

  defmodule SubField do
    @moduledoc """
    Subfield -- just a one-character code and a value.
    Note that subfields can repeat within a tag. This
    makes the list of subfields a straight-up KeywordList
    """
    def new(code, value) do
      {code, value}
    end

    def code(sf), do: elem(sf, 0)
    def value(sf), do: elem(sf, 1)

    def matches_one_of(sf, codes) do
      Enum.any?(codes, fn(c) -> c == SubField.code(sf) end)
    end

  end

  defmodule DataField do
    @moduledoc """
    A DataField consists of a tag (like a control field),
    two "indicators" (blank or a single digit) and an
    ordered set of key-value pairs (the subfields).
    """
    defstruct tag: nil, ind1: nil, ind2: nil, subfields: []


    defimpl Field, for: DataField do
      @doc """
      The default value of a datafield is the values of all the
      subfields joined with spaces. You can pass in a joiner if you'd
      like.
      """
      def value(f, joiner \\ " ") do
        f.subfields
          |> Keyword.values
          |> Enum.join(joiner)
      end

      @doc "Type is :data"
      def type(f), do: :data
    end

    @doc """
    Return a list of the subfields
    """

    def subfields(datafield), do: datafield.subfields

    @doc """
    Return a list of the subfields that match
    any of the list of codes given
    """

    def subfields(datafield, codes) when is_list(codes) do
      datafield
        |> DataField.subfields
        |> Enum.filter(fn(sf) -> SubField.matches_one_of(sf, codes) end)
    end

    @doc """
    Return a list of all subfields that have the given code
    """
    def subfields(datafield, code) do
      subfields(datafield, [code])
    end


    @doc """
    Return the value of the first subfield that matches the code.
    If the code doesn't exist, return an empty string
    or the supplied default
    """
    def firstValue(datafield, code, default \\ "") do
      datafield
        |> DataField.subfields
        |> Keyword.get(code, default)
    end

    @doc """
    Get all values of all the subfields, in order, as a list
    """
    def values(f) do
      f
        |> DataField.subfields
        |> Enum.map(&SubField.value/1)
    end

    @doc """
    Get values for any of the codes, in order,
    in a list.
    """
    def values(datafield, codes) when is_list(codes) do
      datafield
        |> DataField.subfields(codes)
        |> Enum.map(&SubField.value/1)
    end

    @doc """
    Get values for a single code, as a list
    """
    def values(datafield, code) do
      values(datafield, [code])
    end


    @doc """
    Get values that match the code as a list
    """
    def values(f, code) do
      Keyword.get_values(f, code)
    end



  end



  defmodule MIJStream do
    @moduledoc """
    Given the filename of a file with one marc-in-json
    record per line, produce a stream of records
    """

    defp singleMapToKV(m) do
      tag = m |> Map.keys |> List.first
      value = m |> Map.values |> List.first
      {tag, value}
    end

      def open(filename) do
        filename
          |> File.stream!
          |> Stream.map(&Poison.decode/1)
          |> Stream.map(&toRecord/1)
      end

      def toRecord({:ok, m}) do
        toRecord(m)
      end

      def toRecord(m) do
        %Record{
          leader: m["leader"],
          fields: m["fields"] |> Enum.map(&toField/1)
        }
      end

      @doc "Turn an MIJ fieldspec into a controlfield or datafield"
      def toField(m) do
        {tag, value} = singleMapToKV(m)
        toField(tag, value)
      end

      @doc "Turn a MIJ field into a ControlField"
      def toField(tag, value) when (not is_map(value)) do
        %ControlField{tag: tag, value: value}
      end

      @doc "Turn a MIJ field into a DataField"
      def toField(tag, data) when is_map(data) do
        %{"subfields" => subfields, "ind1" => ind1, "ind2" => ind2} = data
        %Marc.DataField{tag: tag, ind1: ind1, ind2: ind2, subfields: Enum.map(subfields, &toSubField/1)}
      end

      @doc "Turn an MIJ subfield into a SubField"
      def toSubField(m) do
        {code, value} = singleMapToKV(m)
        Marc.SubField.new(code, value)
      end

    end
  end

# IO.puts "Starting..."
# count = "10k.json"
#   |> Marc.MIJStream.open
#   |> Stream.map(&Marc.Record.find(&1, "245"))
#   |> Stream.map(&Marc.Field.value/1)
#   |> Enum.count
#
# IO.puts "Found #{count} records"
