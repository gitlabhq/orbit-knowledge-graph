# Base model class for all application models
class BaseModel
  attr_reader :id, :created_at, :updated_at

  def initialize(attributes = {})
    @id = attributes[:id] || generate_id
    @created_at = attributes[:created_at] || Time.now
    @updated_at = attributes[:updated_at] || Time.now
    @attributes = attributes
  end

  def self.find(id)
    storage.find { |record| record.id == id }
  end

  def self.all
    storage.dup
  end

  def self.where(conditions)
    storage.select { |record| conditions.all? { |key, value| record.send(key) == value } }
  end

  def self.create(attributes)
    instance = new(attributes)
    instance.save
    instance
  end

  def save
    touch
    if persisted?
      update_in_storage
    else
      add_to_storage
    end
    self
  end

  def update(attributes)
    @attributes.merge!(attributes)
    attributes.each do |key, value|
      instance_variable_set("@#{key}", value) if respond_to?(key)
    end
    save
  end

  def destroy
    self.class.storage.delete_if { |record| record.id == @id }
    freeze
  end

  def persisted?
    self.class.storage.any? { |record| record.id == @id }
  end

  def to_h
    instance_variables.each_with_object({}) do |var, hash|
      key = var.to_s.delete('@').to_sym
      hash[key] = instance_variable_get(var)
    end
  end

  private

  def generate_id
    SecureRandom.uuid
  end

  def touch
    @updated_at = Time.now
  end

  def self.storage
    @storage ||= []
  end

  def add_to_storage
    self.class.storage << self
  end

  def update_in_storage
    index = self.class.storage.find_index { |record| record.id == @id }
    self.class.storage[index] = self if index
  end
end 
