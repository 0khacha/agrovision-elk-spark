# AgroVision TP3 - validation, classification, conversion (resultat = [@metadata][status])
# Regles : champs obligatoires (MISSING_FIELD), date yyyy-MM-dd existante (INVALID_DATE),
# plages numeriques (INVALID_NUMERIC), modalites autorisees (UNKNOWN_CATEGORY),
# doublon d'observation_id (DUPLICATE), ligne CSV illisible (MALFORMED_ROW).
require 'date'
require 'digest'

def register(params)
  @run_id   = params["run_id"] || "manual"
  @mutex    = Mutex.new
  @seen     = Hash.new(0)   # protege par Mutex ; sert au reporting, la correction repose sur l'_id
  @regions  = %w[Souss-Massa Marrakech-Safi Fes-Meknes Gharb Oriental Doukkala]
  @crops    = %w[ble orge tomate agrumes olivier mais]
  @statuses = %w[OK LOW_BATTERY MAINTENANCE]
  @required = %w[observation_id field_id region crop_type observation_date ndvi soil_moisture temperature sensor_id]
  @columns  = %w[observation_id field_id region crop_type observation_date ndvi soil_moisture temperature
                 rainfall_mm irrigation_liters sensor_id sensor_status area_ha alert_flag notes]
  @ranges   = { "ndvi" => [0.0, 1.0], "soil_moisture" => [0.0, 100.0], "temperature" => [-10.0, 55.0],
                "rainfall_mm" => [0.0, 300.0], "irrigation_liters" => [0.0, 100000.0], "area_ha" => [0.0001, 1000.0] }
end

def filter(event)
  problems = []
  tags = event.get("tags")
  if tags.is_a?(Array) && tags.include?("_csvparsefailure")
    problems << ["MALFORMED_ROW", "CSV line could not be parsed"]
  else
    @columns.each do |f|
      v = event.get(f)
      event.set(f, v.strip) if v.is_a?(String)
    end
    event.set("crop_type", event.get("crop_type").downcase) if event.get("crop_type").is_a?(String)
    event.set("sensor_status", event.get("sensor_status").upcase) if event.get("sensor_status").is_a?(String)
    event.set("alert_flag", event.get("alert_flag").downcase) if event.get("alert_flag").is_a?(String)

    @required.each { |f| problems << ["MISSING_FIELD", "#{f} is empty"] if event.get(f).to_s.empty? }

    d = event.get("observation_date").to_s
    unless d.empty?
      m = /\A(\d{4})-(\d{2})-(\d{2})\z/.match(d)
      unless m && Date.valid_date?(m[1].to_i, m[2].to_i, m[3].to_i)
        problems << ["INVALID_DATE", "observation_date=#{d}"]
      end
    end

    @ranges.each do |f, (lo, hi)|
      v = event.get(f).to_s
      next if v.empty?
      begin
        x = Float(v)
        problems << ["INVALID_NUMERIC", "#{f}=#{v} outside #{lo}..#{hi}"] if x < lo || x > hi
      rescue ArgumentError
        problems << ["INVALID_NUMERIC", "#{f}=#{v} is not numeric"]
      end
    end

    { "region" => @regions, "crop_type" => @crops, "sensor_status" => @statuses }.each do |f, allowed|
      v = event.get(f).to_s
      problems << ["UNKNOWN_CATEGORY", "#{f}=#{v} not allowed"] if !v.empty? && !allowed.include?(v)
    end

    if problems.empty?
      oid = event.get("observation_id")
      occ = @mutex.synchronize { @seen[oid] += 1 }
      event.set("[@metadata][occurrence]", occ)
      problems << ["DUPLICATE", "observation_id #{oid} seen #{occ} times in this run"] if occ > 1
    end
  end

  src = event.get("[log][file][path]").to_s
  event.set("source_file", File.basename(src)) unless src.empty?
  event.set("run_id", @run_id)

  if problems.empty?
    %w[ndvi soil_moisture temperature rainfall_mm area_ha].each do |f|
      v = event.get(f).to_s
      event.set(f, Float(v)) unless v.empty?
    end
    v = event.get("irrigation_liters").to_s
    event.set("irrigation_liters", Float(v).to_i) unless v.empty?
    event.set("alert_flag", event.get("alert_flag") == "true")
    @columns.each { |f| event.remove(f) if event.get(f).is_a?(String) && event.get(f).empty? }
    event.set("[@metadata][status]", "valid")
  else
    cat = problems.first[0]
    event.set("anomaly_category", cat)
    event.set("anomaly_detail", problems.map { |c, t| "#{c}: #{t}" }.join("; "))
    event.set("error_class", "DATA_QUALITY")
    occ = event.get("[@metadata][occurrence]") || 1
    key = "#{event.get('source_file')}|#{event.get('raw_line')}|#{cat}|#{occ}"
    event.set("[@metadata][reject_id]", Digest::SHA1.hexdigest(key))
    event.set("[@metadata][status]", "rejected")
  end
  [event]
end
