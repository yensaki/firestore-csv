require "google/cloud/firestore"
require 'csv'

class Import
  def initialize
    project_id = "yen-sakiika"
    @firestore = Google::Cloud::Firestore.new(project_id: project_id)
  end

  def update_special_weapons
    update_collection("special_weapons", "special_weapons.csv")
  end

  def update_sub_weapons
    update_collection("sub_weapons", "sub_weapons.csv")
  end

  def update_weapons
    update_collection("weapons", "weapons.csv")
  end

  private

  def update_collection(collection_name, filename)
    collection = @firestore.collection(collection_name)
    collection.list_documents.each do |document|
      document.delete
    end

    csv = CSV.read("./fixtures/#{filename}", headers: true)
    csv.each.with_index(1) do |row, index|
      collection.doc("#{index}").create(build_document_attributes(row)) rescue nil
    end
  end

  def build_document_attributes(row)
    row.map do |label, value|
      field_name, _value_type = label.split(":")
      [field_name, value]
    end.to_h
  end
end

import = Import.new
import.update_special_weapons
import.update_sub_weapons
import.update_weapons
