module VMPerformance

using DataFrames
using Arrow
using Dates
using StatsBase

export clean_education
#Main entry point
function julia_main()::Cint
    clean_education()
    return 0 # if things finished successfully
end

#region functions
const agelevels = Dict{Int64,Int16}(5 => 1, 6 => 2, 7 => 3, 8 => 4, 9 => 5, 10 => 6,
    11 => 7, 12 => 8, 13 => 9, 14 => 10, 15 => 11, 16 => 12)

"""
    clean_education(; odb = nothing)::AbstractDataFrame

Return a dataframe with cleaned education status in the CleanedEducation column
"""
function clean_education()::AbstractDataFrame
    println("Started cleaning education status")
    start_time = now()
    education = educationstatus_prepare()
    education_clean!(education)
    Arrow.write("CleanedEducation.arrow", education)
    println("=== Finished cleaning education status after $(canonicalize(Dates.CompoundPeriod(Dates.now() - start_time)))")
    return education
end
"""
    education_clean!(education::AbstractDataFrame)

Update the CleanedEducation column in the education dataframe with the cleaned education status
"""
function education_clean!(education::AbstractDataFrame)
    individuals = groupby(education, :IndividualId)
    for individual in individuals
        cleaned = individualeducation_clean(collect(individual.Age), collect(individual.EducationStatus))
        individual.CleanedEducation = cleaned
    end
    return nothing
end

"""
    expandbounds(start_date, end_date)

Return a vector of years between the start_date and end_date
"""
function expandbounds(start_date, end_date)
    start_year = Dates.year(start_date)
    end_year = Dates.year(end_date)
    Year = collect(start_year:end_year)
    return Year
end
"""
    educationstatus_prepare(db::DuckDB.DB)

Create a record for every calendar year within the IndividualBounds for each individual.
Record the highest school level attained by the individual in that year. Indicate if more than 1 education status was recorded in a year.
"""
function educationstatus_prepare()::AbstractDataFrame
    bounds = Arrow.Table("IndividualBounds.arrow") |> DataFrame
    boundyrs = combine(bounds, :IndividualId => identity => :IndividualId, [:EarliestDate, :LatestDate] => ByRow((x, y) -> expandbounds(x, y)) => :Year)
    boundyrs = flatten(boundyrs, :Year)
    individuals = Arrow.Table("Individuals.arrow") |> DataFrame
    select!(individuals, [:IndividualId, :DoB])
    boundyrs = leftjoin(boundyrs, individuals, on=:IndividualId)
    subset!(boundyrs, [:DoB, :Year] => ByRow((x, y) -> y - Dates.year(x) >= 0))
    transform!(boundyrs, [:DoB, :Year] => ByRow((x, y) -> y - Dates.year(x)) => :Age)
    education = Arrow.Table("EducationStatuses.arrow") |> DataFrame
    transform!(education, :ObservationDate => ByRow(x -> Dates.year(x)) => :Year)
    educationyr = combine(groupby(education, [:IndividualId, :Year]), :HighestSchoolLevel => mode => :EducationStatus)
    education = leftjoin(boundyrs, educationyr, on=[:IndividualId, :Year])
    DataFrames.sort!(education, [:IndividualId, :Year])
    education[!, :CleanedEducation] = Vector{Union{Missing,Int16}}(missing, size(education, 1))

    return education
end
"""
    maxlevelatage(age::Integer)::Integer

Return the maximum feasible education level for a given age
"""
function maxlevelatage(age::Int64)::Int16
    if age > 16
        return Int16(12)
    end
    if age < 5
        return Int16(0)
    end
    return agelevels[age]
end

function education_validate(age::Vector{Int64}, education::Vector{Union{Missing,Int16}})::Bool
    prevmissing = false
    lastnonmissing = 0 #index of last non-mising education level
    lastlevel = -1
    valid = true
    for i in eachindex(education)
        if ismissing(education[i]) || education[i] == -1
            prevmissing = true
            continue
        else
            prevmissing = false
        end
        if education[i] > maxlevelatage(age[i]) && education[i] != 98
            valid = false
            #@info "Education too high for age, age: $(age[i]) education: $(education[i])"
            break
        end
        # Only proceed if there is a last nonmissing level
        if lastnonmissing == 0
            lastlevel = education[i]
            lastnonmissing = i
            continue
        end
        # the increase from one education level to the next non-missing level 
        # cannot be more than the number of steps in between
        if education[i] != 98 && (education[i] - lastlevel) > (i - lastnonmissing)
            valid = false
            #@info "Education increment too high, i: $(i) age: $(age[i]) education: $(education[i]) lastnonmissing: $(lastnonmissing) lastlevel: $lastlevel"
            break
        end
        # education level cannot reduce
        if education[i] < lastlevel
            valid = false
            #@info "Education regress, i: $(i) age: $(age[i]) education: $(education[i]) lastnonmissing: $(lastnonmissing) lastlevel: $lastlevel"
            break
        end
        lastlevel = education[i]
        lastnonmissing = i
    end
    return valid
end
function interpolate_missing_values(vec)
    non_missing_indices = findall(!ismissing, vec)
    # Check if the vector starts with missing values and the first non-missing value is 98
    if !isempty(non_missing_indices) && vec[non_missing_indices[1]] == 98
        for i in 1:non_missing_indices[1]-1
            vec[i] = 98
        end
    end
    for i in 1:length(non_missing_indices)-1
        start_idx = non_missing_indices[i]
        end_idx = non_missing_indices[i+1]
        # Check if there are missing values to interpolate
        if end_idx - start_idx > 1
            start_val = vec[start_idx]
            end_val = vec[end_idx]
            if start_val == 98 && end_val != 98
                continue #Do not extrapolate 98 to a lower value
            end
            if end_val == 98 #never went to school, fill with 98
                for j in start_idx+1:end_idx-1
                    vec[j] = 98
                end
            else
                # Linear interpolation
                for j in start_idx+1:end_idx-1
                    value = start_val + (end_val - start_val) * ((j - start_idx) / (end_idx - start_idx))
                    vec[j] = round(Int, value)
                end
            end
        end
    end
    # Handle case where the last non-missing value equals 12
    if !isempty(non_missing_indices) && vec[non_missing_indices[end]] == 12
        for i in non_missing_indices[end]+1:length(vec)
            if ismissing(vec[i])
                vec[i] = 12
            end
        end
    end

    return vec
end
function individualeducation_clean(age::Vector{Int64}, education::Vector{Union{Missing,Int16}})::Vector{Union{Missing,Int16}}
    prevmissing = false
    lastnonmissing = 0 #index of last non-mising education level
    lastlevel = -1
    cleaned = Vector{Union{Missing,Int16}}(undef, length(education))
    skipindex = -1
    for i in eachindex(education)
        if ismissing(education[i]) || education[i] == -1
            prevmissing = true
            cleaned[i] = missing
            continue
        else
            prevmissing = false
        end
        if education[i] > maxlevelatage(age[i]) && education[i] != 98
            cleaned[i] = missing
            #@info "Education too high for age, age: $(age[i]) education: $(education[i])"
            continue
        elseif age[i] < 10 && education[i] == 98
            # Never been to school before age 10 set to missing
            cleaned[i] = missing
            continue
        end
        # Only proceed if there is a last nonmissing level
        if lastnonmissing == 0
            if age[i] >= 25 #use alternative approach for age over 25
                skipindex = i
                break
            else
                lastlevel = education[i]
                lastnonmissing = i
                cleaned[i] = lastlevel
                continue
            end
        end
        # the increase from one education level to the next non-missing level 
        # cannot be more than the number of steps in between
        if education[i] == 98 #Cannot never went to school if there is a prior education level
            cleaned[i] = missing
        elseif (education[i] - lastlevel) > (i - lastnonmissing)
            cleaned[i] = missing
            #@info "Education increment too high, i: $(i) age: $(age[i]) education: $(education[i]) lastnonmissing: $(lastnonmissing) lastlevel: $lastlevel"
        elseif education[i] < lastlevel && lastlevel != 98
            # education level cannot reduce
            cleaned[i] = missing
            #@info "Education regress, i: $(i) age: $(age[i]) education: $(education[i]) lastnonmissing: $(lastnonmissing) lastlevel: $lastlevel"
        else
            lastlevel = education[i]
            cleaned[i] = lastlevel
            lastnonmissing = i
        end
    end
    # set education status from skipindex onwards to the mode of the observed education status
    # over the range skipindex:end
    if skipindex > 0
        filtered_education = collect(skipmissing(education[skipindex:end]))
        if !isempty(filtered_education)
            mode_education = mode(filtered_education)
            cleaned[skipindex:end] .= mode_education
        end
    end
    return interpolate_missing_values(cleaned)
end

#endregion

end
