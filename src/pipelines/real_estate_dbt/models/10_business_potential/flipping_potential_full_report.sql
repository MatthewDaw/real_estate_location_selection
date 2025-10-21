select 
fp.source_id,
hd.listing_urls,
hd.distance_miles,
hd.predicted_current_effective_price_per_sqft,
hd.leased_percent
-- hd.predicted_current_effective_price_per_sqft * hd.leased_percent as revenue_per_sqft
from {{ ref('flipping_potential_v2') }} fp
join {{ ref('hd_explanations') }} hd on fp.source_id = hd.source_id

