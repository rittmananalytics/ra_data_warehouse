with source as (
  select
    *,
    max(_sdc_sequence) over (partition by id order by _sdc_sequence range between unbounded preceding and unbounded following) as max_sdc_sequence
  from {{ source('everhour', 'users') }}
),

renamed as (
  select
    --g.name as team_name,
    --g.id as team_id,
    rate as daily_billable_rate,
    cost as daily_cost,
    status as is_active,
    headline as role_name,
    role as everhour_role,
    email as team_member_email,
    createdat as created_at,
    name as team_member_name,
    avatarUrl as everhour_avatarUrl,
    capacity,
    id as team_member_id,
  from source s
  where _sdc_sequence = max_sdc_sequence
)

select * from renamed
