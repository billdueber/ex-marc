defmodule Marc do
  @moduledoc """
  # Abstract MARC21 library for Elixir

  Bare-bones MARC routines for dealing with library MARC
  records in the most common format (single-char indicators,
  three-character tags, single-char subfield codes, all UTF-8)
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

    def fields(r, filter) when is_list(filter) do
      Enum.filter(r.fields, fn(f) -> Enum.any?(filter, &(Marc.Field.tag(f) == &1)) end)
    end

    def fields(r, str) when is_binary(str) do
      fields(r, [str])
    end

  end

  defprotocol Field do
    @doc "Return string value of field"
    def value(field)

    @doc "Report the field type"
    def type(field)

    @doc "Use a default implementation for tag"
    defdelegate tag(field), to: Field.Util
  end

  @doc "Default implementation of `tag(field)`"
  defmodule  Field.Util do
    def tag(f), do: f.tag

  end

  @doc "A ControlField -- just a tag and a value"
  defmodule ControlField do
    defstruct tag: nil, value: nil

    defimpl Field, for: ControlField do
      @derive Field
      def value(f) do
        f.value
      end

      @doc "Type is :control"
      def type(f), do: :control
    end


  end

  defmodule SubField do
    defstruct code: nil, value: nil
    def code(sf), do: sf.code
    def value(sf), do: sf.value

  end

  defmodule DataField do
    defstruct tag: nil, ind1: nil, ind2: nil, subfields: []

    defimpl Field, for: DataField do
      @derive Field
      def value(f) do
        f.subfields |> Enum.map(&(&1.value)) |> Enum.join(" ")
      end

      @doc "Type is :data"
      def type(f), do: :data

    end

    @doc "Return the value of the first subfield that matches the tag"
    def subfieldValue(f, code) do
      f.subfields |> Enum.find(&(&1.code == code)) |> Marc.SubField.value
    end

  end



  defmodule MIJStream do

      def open(filename) do
        File.stream!(filename)
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
        tag = Map.keys(m) |> List.first
        value = Map.values(m) |> List.first
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
        code = Map.keys(m) |> List.first
        value = Map.values(m) |> List.first
        %Marc.SubField{code: code, value: value}
      end

    end
  end
