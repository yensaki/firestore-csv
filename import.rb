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
    @firestore.transaction do |transaction|
      ids = []
      csv = CSV.read("./fixtures/#{filename}", headers: true)
      csv.each do |row|
        attributes = build_document_attributes(row)
        transaction.set("#{collection_name}/#{attributes["id"]}", attributes)
        ids << attributes["id"].to_s
      end
      all_ids = []
      @firestore.collection(collection_name).list_documents.all.each { |doc_ref| all_ids << doc_ref.document_id }
      delete_ids = (all_ids - ids)
      delete_ids.each do |delete_id|
        transaction.delete("#{collection_name}/#{delete_id}")
      end
    end
    puts "#{collection_name} done"
    sleep(3)
  end

  def build_document_attributes(row)
    row.map do |label, value|
      field_name, value_type, reference_collection = label.split(":")
      [field_name, convert_value_type(value, value_type, reference_collection)]
    end.to_h
  end

  def convert_value_type(value, value_type, reference_collection)
    return value if value.to_s.empty?

    case value_type&.to_sym
    when :integer
      value.to_i
    when :reference
      @firestore.document("#{reference_collection}/#{value}")
    else
      value
    end
  end
end

import = Import.new
import.update_special_weapons
import.update_sub_weapons
import.update_weapons
