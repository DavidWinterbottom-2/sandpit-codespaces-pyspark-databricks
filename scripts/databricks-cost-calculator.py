#!/usr/bin/env python3
"""
Azure Databricks Cost Calculator
Calculates estimated monthly costs for different scenarios
"""

import argparse
from typing import Any, Dict

# Azure VM pricing (East US, Linux, pay-as-you-go, approximate)
VM_PRICING = {
    "Standard_DS3_v2": 0.192,    # 4 vCPUs, 14 GB RAM
    "Standard_DS4_v2": 0.384,    # 8 vCPUs, 28 GB RAM
    "Standard_DS5_v2": 0.768,    # 16 vCPUs, 56 GB RAM
    "Standard_E4s_v3": 0.252,    # 4 vCPUs, 32 GB RAM (memory optimized)
    "Standard_E8s_v3": 0.504,    # 8 vCPUs, 64 GB RAM (memory optimized)
    "Standard_F4s_v2": 0.169,    # 4 vCPUs, 8 GB RAM (compute optimized)
    "Standard_F8s_v2": 0.338,    # 8 vCPUs, 16 GB RAM (compute optimized)
}

# Databricks pricing per DBU (USD)
DATABRICKS_PRICING = {
    "trial": 0.0,      # Free for 14 days
    "standard": 0.40,   # Standard tier
    "premium": 0.55,    # Premium tier
}

# DBU consumption rates
DBU_RATES = {
    "interactive": 1.0,    # DBU per node per hour for interactive clusters
    "job": 0.5,           # DBU per node per hour for job clusters
    "sql_warehouse": {    # DBU per hour based on size
        "2X-Small": 1.0,
        "X-Small": 2.0,
        "Small": 4.0,
        "Medium": 8.0,
        "Large": 16.0,
        "X-Large": 32.0,
        "2X-Large": 64.0,
        "3X-Large": 128.0,
    }
}


def calculate_cluster_cost(
    vm_type: str,
    num_nodes: int,
    hours_per_day: int,
    days_per_month: int,
    tier: str,
    cluster_type: str = "interactive"
) -> Dict[str, float]:
    """Calculate monthly cost for a Databricks cluster"""

    if vm_type not in VM_PRICING:
        raise ValueError(f"Unknown VM type: {vm_type}")

    if tier not in DATABRICKS_PRICING:
        raise ValueError(f"Unknown tier: {tier}")

    # Calculate total hours
    total_hours = hours_per_day * days_per_month

    # Calculate VM costs
    vm_cost_per_hour = VM_PRICING[vm_type]
    total_vm_cost = num_nodes * vm_cost_per_hour * total_hours

    # Calculate DBU costs
    dbu_rate = DBU_RATES.get(cluster_type, 1.0)
    dbu_price = DATABRICKS_PRICING[tier]
    total_dbu_hours = num_nodes * total_hours * dbu_rate
    total_dbu_cost = total_dbu_hours * dbu_price

    return {
        "vm_cost": total_vm_cost,
        "dbu_cost": total_dbu_cost,
        "total_cost": total_vm_cost + total_dbu_cost,
        "total_dbu_hours": total_dbu_hours,
        "total_vm_hours": num_nodes * total_hours
    }


def format_cost_summary(scenario: str, config: Dict[str, Any], costs: Dict[str, float]):
    """Format cost summary for display"""
    print(f"\n{'='*60}")
    print(f"💰 {scenario}")
    print(f"{'='*60}")
    print("Configuration:")
    print(f"  • VM Type: {config['vm_type']}")
    print(f"  • Nodes: {config['num_nodes']}")
    print(f"  • Hours/day: {config['hours_per_day']}")
    print(f"  • Days/month: {config['days_per_month']}")
    print(f"  • Tier: {config['tier']}")
    print(f"  • Cluster type: {config.get('cluster_type', 'interactive')}")
    print("\nCosts:")
    print(f"  • Azure VM cost: ${costs['vm_cost']:.2f}")
    print(f"  • Databricks DBU cost: ${costs['dbu_cost']:.2f}")
    print(f"  • Total monthly cost: ${costs['total_cost']:.2f}")
    print("\nUsage:")
    print(f"  • Total VM hours: {costs['total_vm_hours']:.0f}")
    print(f"  • Total DBU hours: {costs['total_dbu_hours']:.0f}")


def compare_tiers(vm_type: str, num_nodes: int, hours_per_day: int, days_per_month: int):
    """Compare costs across different pricing tiers"""
    print("\n🔍 Tier Comparison")
    print("=" * 80)

    configs = [
        {"tier": "standard", "name": "Standard Tier"},
        {"tier": "premium", "name": "Premium Tier"}
    ]

    results = []
    for config in configs:
        cost_config = {
            "vm_type": vm_type,
            "num_nodes": num_nodes,
            "hours_per_day": hours_per_day,
            "days_per_month": days_per_month,
            "tier": config["tier"],
        }
        costs = calculate_cluster_cost(**cost_config)
        results.append({
            "name": config["name"],
            "tier": config["tier"],
            "total_cost": costs["total_cost"],
            "dbu_cost": costs["dbu_cost"],
            "vm_cost": costs["vm_cost"]
        })

    print(f"{'Tier':<15} {'VM Cost':<12} {'DBU Cost':<12} {'Total':<12} {'vs Standard':<12}")
    print("-" * 80)

    standard_cost = next(r["total_cost"]
                         for r in results if r["tier"] == "standard")

    for result in results:
        diff = result["total_cost"] - standard_cost
        diff_pct = (diff / standard_cost) * 100 if standard_cost > 0 else 0
        diff_str = f"+${diff:.0f} ({diff_pct:+.0f}%)" if diff != 0 else "baseline"

        print(f"{result['name']:<15} ${result['vm_cost']:<11.2f} ${result['dbu_cost']:<11.2f} ${result['total_cost']:<11.2f} {diff_str:<12}")


def main():
    parser = argparse.ArgumentParser(
        description="Azure Databricks Cost Calculator")
    parser.add_argument("--vm-type", default="Standard_DS3_v2", choices=VM_PRICING.keys(),
                        help="Azure VM type")
    parser.add_argument("--nodes", type=int, default=3,
                        help="Number of cluster nodes")
    parser.add_argument("--hours-per-day", type=int,
                        default=8, help="Hours per day")
    parser.add_argument("--days-per-month", type=int,
                        default=22, help="Working days per month")
    parser.add_argument("--tier", choices=DATABRICKS_PRICING.keys(), default="standard",
                        help="Databricks pricing tier")
    parser.add_argument("--cluster-type", choices=["interactive", "job"], default="interactive",
                        help="Cluster type (affects DBU consumption)")
    parser.add_argument("--compare", action="store_true",
                        help="Compare all tiers")
    parser.add_argument("--scenarios", action="store_true",
                        help="Show common scenarios")

    args = parser.parse_args()

    print("🧮 Azure Databricks Cost Calculator")
    print("=" * 50)

    if args.scenarios:
        # Common scenarios
        scenarios = [
            {
                "name": "Development Environment",
                "config": {
                    "vm_type": "Standard_DS3_v2",
                    "num_nodes": 2,
                    "hours_per_day": 6,
                    "days_per_month": 22,
                    "tier": "standard",
                    "cluster_type": "interactive"
                }
            },
            {
                "name": "Production ETL (Job Cluster)",
                "config": {
                    "vm_type": "Standard_DS4_v2",
                    "num_nodes": 4,
                    "hours_per_day": 2,
                    "days_per_month": 30,
                    "tier": "premium",
                    "cluster_type": "job"
                }
            },
            {
                "name": "Data Science Team",
                "config": {
                    "vm_type": "Standard_E4s_v3",
                    "num_nodes": 3,
                    "hours_per_day": 8,
                    "days_per_month": 22,
                    "tier": "standard",
                    "cluster_type": "interactive"
                }
            }
        ]

        for scenario in scenarios:
            costs = calculate_cluster_cost(**scenario["config"])
            format_cost_summary(scenario["name"], scenario["config"], costs)

    elif args.compare:
        compare_tiers(args.vm_type, args.nodes,
                      args.hours_per_day, args.days_per_month)

    else:
        # Single calculation
        config = {
            "vm_type": args.vm_type,
            "num_nodes": args.nodes,
            "hours_per_day": args.hours_per_day,
            "days_per_month": args.days_per_month,
            "tier": args.tier,
            "cluster_type": args.cluster_type
        }

        costs = calculate_cluster_cost(**config)
        format_cost_summary("Custom Configuration", config, costs)

    print("\n💡 Cost Optimization Tips:")
    print("  • Use job clusters (50% cheaper DBUs) for scheduled workloads")
    print("  • Enable auto-termination to avoid idle cluster costs")
    print("  • Consider spot instances for non-critical workloads")
    print("  • Right-size your clusters based on actual usage")
    print("  • Standard tier is often sufficient for development/testing")

    print("\n📊 VM Type Options:")
    for vm, price in VM_PRICING.items():
        print(f"  • {vm}: ${price}/hour per node")


if __name__ == "__main__":
    main()
